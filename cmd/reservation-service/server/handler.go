package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	pb "github.com/murraystewart96/shippy/proto/reservation"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/storage"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GRPCHandler struct {
	pb.UnimplementedReservationServiceServer
	vesselCli vesselpb.VesselServiceClient
	cache     storage.ReservationCache
}

func NewGRPCHandler(vesselCli vesselpb.VesselServiceClient, cache storage.ReservationCache) *GRPCHandler {
	return &GRPCHandler{
		vesselCli: vesselCli,
		cache:     cache,
	}
}

func (h *GRPCHandler) ReserveCapacity(ctx context.Context, req *pb.ReserveCapacityRequest) (*pb.ReservationResponse, error) {
	reservationID := uuid.New()
	log.Info().
		Str("reservation_id", reservationID.String()).
		Int32("weight", req.Weight).
		Int32("number_of_containers", req.NumberOfContainers).
		Msg("ReserveCapacity: calling vessel service")

	spec := &vesselpb.Specification{
		ReservationId:      reservationID.String(),
		NumberOfContainers: req.NumberOfContainers,
		Weight:             req.Weight,
	}

	res, err := h.vesselCli.ReserveCapacity(ctx, spec)
	if err != nil {
		log.Error().Str("reservation_id", reservationID.String()).Err(err).Msg("ReserveCapacity: vessel call failed")
		return nil, fmt.Errorf("vessel ReserveCapacity failed: %w", err)
	}

	vesselID, err := uuid.Parse(res.Vessel.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to convert vessel ID %s to uuid: %w", res.Vessel.Id, err)
	}

	consignmentID, err := uuid.Parse(req.ConsignmentId)
	if err != nil {
		return nil, fmt.Errorf("failed to convert consignment ID %s to uuid: %w", req.ConsignmentId, err)
	}

	// Cache reservation info
	err = h.cache.Store(ctx, reservationID.String(), storage.ReservationInfo{
		Id:                 reservationID,
		ConsignmentID:      consignmentID,
		VesselID:           vesselID,
		NumberOfContainers: int(req.NumberOfContainers),
		Weight:             int(req.Weight),
	})
	if err != nil {
		// Compensate — release the capacity we just reserved
		bo := backoff.WithMaxRetries(backoff.NewExponentialBackOff(
			backoff.WithInitialInterval(100*time.Millisecond),
		), 3)
		if releaseErr := backoff.Retry(func() error {
			_, err := h.vesselCli.ReleaseCapacity(ctx, &vesselpb.CapacityRequest{
				ReservationId:      reservationID.String(),
				VesselId:           res.Vessel.Id,
				Weight:             req.Weight,
				NumberOfContainers: req.NumberOfContainers,
			})
			return err
		}, bo); releaseErr != nil {
			log.Error().
				Str("reservation_id", reservationID.String()).
				Err(releaseErr).
				Msg("ALERT: failed to release capacity after cache failure — manual release required")
		}
		return nil, fmt.Errorf("failed to cache reservation: %w", err)
	}

	log.Info().
		Str("reservation_id", reservationID.String()).
		Str("vessel_id", vesselID.String()).
		Msg("ReserveCapacity: reservation created and cached")

	return &pb.ReservationResponse{
		Id:       spec.ReservationId,
		VesselId: vesselID.String(),
		Reserved: true,
	}, nil
}

func (h *GRPCHandler) ReleaseCapacity(ctx context.Context, req *pb.CapacityActionRequest) (*pb.Empty, error) {
	log.Info().Str("reservation_id", req.Id).Msg("ReleaseCapacity: received")

	reservation, err := h.cache.GetData(ctx, req.Id)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			log.Warn().Str("reservation_id", req.Id).Msg("ReleaseCapacity: reservation not found in cache")
			return nil, status.Error(codes.NotFound, "reservation not found")
		}
		return nil, status.Error(codes.Internal, "failed to get reservation")
	}

	_, err = h.vesselCli.ReleaseCapacity(ctx, &vesselpb.CapacityRequest{
		ReservationId:      reservation.Id.String(),
		VesselId:           reservation.VesselID.String(),
		Weight:             int32(reservation.Weight),
		NumberOfContainers: int32(reservation.NumberOfContainers),
	})
	if err != nil {
		log.Error().Str("reservation_id", req.Id).Err(err).Msg("ReleaseCapacity: vessel call failed")
		return nil, fmt.Errorf("vessel ReleaseCapacity failed: %w", err)
	}

	log.Info().
		Str("reservation_id", reservation.Id.String()).
		Str("vessel_id", reservation.VesselID.String()).
		Msg("ReleaseCapacity: capacity released")

	// Not critical if these fail and are released again by the manager cleanup - vessel release is idempotent
	if _, err := h.cache.DeleteData(ctx, reservation.Id.String()); err != nil {
		log.Warn().Str("reservation id", reservation.Id.String()).Err(err).Msg("failed to delete data from cache")
	}
	if _, err := h.cache.DeleteID(ctx, reservation.Id.String()); err != nil {
		log.Warn().Str("reservation id", reservation.Id.String()).Err(err).Msg("failed to delete id from cache")
	}

	return nil, nil
}

func (h *GRPCHandler) RefreshReservation(ctx context.Context, req *pb.CapacityActionRequest) (*pb.Empty, error) {
	log.Info().Str("reservation_id", req.Id).Msg("RefreshReservation: received")

	refreshed, err := h.cache.Refresh(ctx, req.Id)
	if err != nil {
		log.Error().Str("reservation_id", req.Id).Err(err).Msg("RefreshReservation: cache refresh failed")
		return nil, status.Error(codes.Internal, "failed to refresh reservation")
	}
	if !refreshed {
		log.Warn().Str("reservation_id", req.Id).Msg("RefreshReservation: reservation expired")
		return nil, status.Error(codes.NotFound, "reservation expired")
	}

	log.Info().Str("reservation_id", req.Id).Msg("RefreshReservation: TTL refreshed")
	return &pb.Empty{}, nil
}

func (h *GRPCHandler) ConfirmCapacity(ctx context.Context, req *pb.CapacityActionRequest) (*pb.Empty, error) {
	log.Info().Str("reservation_id", req.Id).Msg("ConfirmCapacity: received")

	reservation, err := h.cache.GetData(ctx, req.Id)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			log.Warn().Str("reservation_id", req.Id).Msg("ConfirmCapacity: reservation not found in cache")
			return nil, status.Error(codes.NotFound, "reservation not found")
		}
		return nil, status.Error(codes.Internal, "failed to get reservation")
	}

	_, err = h.vesselCli.ConfirmCapacity(ctx, &vesselpb.CapacityRequest{
		ReservationId:      reservation.Id.String(),
		VesselId:           reservation.VesselID.String(),
		Weight:             int32(reservation.Weight),
		NumberOfContainers: int32(reservation.NumberOfContainers),
	})
	if err != nil {
		log.Error().Str("reservation_id", req.Id).Err(err).Msg("ConfirmCapacity: vessel call failed")
		return nil, fmt.Errorf("vessel ConfirmCapacity failed: %w", err)
	}

	log.Info().
		Str("reservation_id", reservation.Id.String()).
		Str("vessel_id", reservation.VesselID.String()).
		Msg("ConfirmCapacity: capacity confirmed")

	// Critical — if data key is not deleted the cleanup job will unintentionally release confirmed capacity
	bo := backoff.WithMaxRetries(backoff.NewExponentialBackOff(
		backoff.WithInitialInterval(100*time.Millisecond),
	), 3)
	if err := backoff.Retry(func() error {
		_, err := h.cache.DeleteData(ctx, reservation.Id.String())
		return err
	}, bo); err != nil {
		log.Error().
			Str("reservation_id", reservation.Id.String()).
			Err(err).
			Msg("ALERT: failed to delete reservation data after confirm — manual intervention required to prevent capacity leak")
	}

	// Best effort — TTL will clean it up eventually
	if _, err := h.cache.DeleteID(ctx, reservation.Id.String()); err != nil {
		log.Warn().Str("reservation_id", reservation.Id.String()).Err(err).Msg("failed to delete reservation id from cache")
	}

	return nil, nil
}

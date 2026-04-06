package main

import (
	"context"
	"errors"
	"log"

	pb "github.com/murraystewart96/shippy/proto/vessel"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type handler struct {
	pb.UnimplementedVesselServiceServer
	repository repository
}

func (h *handler) Create(ctx context.Context, req *pb.Vessel) (*pb.Response, error) {
	if err := h.repository.Create(ctx, MarshalVessel(req)); err != nil {
		log.Printf("failed to create vessel: %v\n", err)
		return nil, status.Error(codes.Internal, "failed to create vessel")
	}

	return &pb.Response{Created: true, Vessel: req}, nil
}

func (h *handler) ReserveCapacity(ctx context.Context, req *pb.Specification) (*pb.Response, error) {
	log.Printf("ReserveCapacity: reservation_id=%s weight=%d containers=%d", req.ReservationId, req.Weight, req.NumberOfContainers)

	vessel, err := h.repository.ReserveCapacity(ctx, MarshalSpecification(req))
	if err != nil {
		if errors.Is(err, ErrNoVesselAvailable) {
			log.Printf("ReserveCapacity: no vessel available for reservation_id=%s", req.ReservationId)
			return nil, status.Error(codes.NotFound, "no vessel available for specification")
		}
		log.Printf("ReserveCapacity: failed for reservation_id=%s: %v", req.ReservationId, err)
		return nil, status.Error(codes.Internal, "failed to reserve capacity")
	}

	log.Printf("ReserveCapacity: reserved vessel_id=%s for reservation_id=%s", vessel.ID, req.ReservationId)
	return &pb.Response{Vessel: UnmarshalVessel(vessel)}, nil
}

func (h *handler) ReleaseCapacity(ctx context.Context, req *pb.CapacityRequest) (*pb.Empty, error) {
	log.Printf("ReleaseCapacity: reservation_id=%s vessel_id=%s weight=%d containers=%d", req.ReservationId, req.VesselId, req.Weight, req.NumberOfContainers)

	if err := h.repository.ReleaseCapacity(ctx, MarshalCapacityRequest(req)); err != nil {
		log.Printf("ReleaseCapacity: failed for reservation_id=%s: %v", req.ReservationId, err)
		return nil, status.Error(codes.Internal, "failed to release capacity")
	}

	log.Printf("ReleaseCapacity: released vessel_id=%s for reservation_id=%s", req.VesselId, req.ReservationId)
	return &pb.Empty{}, nil
}

func (h *handler) ConfirmCapacity(ctx context.Context, req *pb.CapacityRequest) (*pb.Empty, error) {
	log.Printf("ConfirmCapacity: reservation_id=%s vessel_id=%s weight=%d containers=%d", req.ReservationId, req.VesselId, req.Weight, req.NumberOfContainers)

	if err := h.repository.ConfirmCapacity(ctx, MarshalCapacityRequest(req)); err != nil {
		log.Printf("ConfirmCapacity: failed for reservation_id=%s: %v", req.ReservationId, err)
		return nil, status.Error(codes.Internal, "failed to confirm capacity")
	}

	log.Printf("ConfirmCapacity: confirmed vessel_id=%s for reservation_id=%s", req.VesselId, req.ReservationId)
	return &pb.Empty{}, nil
}

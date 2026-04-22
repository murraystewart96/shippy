package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/murraystewart96/shippy/consignment-service/manager"
	"github.com/murraystewart96/shippy/consignment-service/storage"
	"github.com/murraystewart96/shippy/consignment-service/storage/mongo"
	pb "github.com/murraystewart96/shippy/proto/consignment"
	paymentpb "github.com/murraystewart96/shippy/proto/payment"
	reservepb "github.com/murraystewart96/shippy/proto/reservation"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type handler struct {
	pb.UnimplementedConsignmentServiceServer
	repository     storage.ConsignmentRepository
	reservationCli reservepb.ReservationServiceClient
	paymentCli     paymentpb.PaymentServiceClient
	outbox         storage.OutboxRepository
}

func (h *handler) CreateConsignment(ctx context.Context, req *pb.Consignment) (*pb.Response, error) {
	reservationResponse, err := h.reservationCli.ReserveCapacity(ctx, &reservepb.ReserveCapacityRequest{
		ConsignmentId:      req.Id,
		Weight:             req.Weight,
		NumberOfContainers: int32(len(req.Containers)),
	})
	if err != nil {
		log.Printf("failed to reserve capacity on vessel: %v\n", err)
		return nil, status.Error(codes.Internal, "failed to reserve capacity")
	}

	req.VesselId = reservationResponse.VesselId

	consignment := mongo.MarshalConsignment(req)
	consignment.ReservationID = reservationResponse.Id
	if err := h.repository.Create(ctx, consignment); err != nil {
		log.Printf("failed to create consignment: %v\n", err)
		return nil, status.Error(codes.Internal, "failed to create consignment")
	}

	return &pb.Response{Created: true, Consignment: mongo.UnmarshalConsignment(consignment)}, nil
}

func (h *handler) ConfirmConsignment(ctx context.Context, req *pb.ConfirmRequest) (*pb.ConfirmResponse, error) {
	// Get consignment
	consignment, err := h.repository.GetByID(ctx, req.Id)
	if err != nil {
		log.Printf("failed to get consignment %s: %v", req.Id, err)
		return nil, status.Error(codes.NotFound, "consignment not found")
	}

	_, err = h.reservationCli.RefreshReservation(ctx, &reservepb.CapacityActionRequest{Id: consignment.ReservationID})
	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.NotFound {
			return nil, status.Error(codes.FailedPrecondition, "reservation has expired")
		}
		log.Printf("failed to refresh reservation %s: %v", consignment.ReservationID, err)
		return nil, status.Error(codes.Internal, "failed to refresh reservation")
	}

	// Authorise payment
	paymentResponse, err := h.paymentCli.Authorise(ctx, &paymentpb.AuthoriseRequest{
		ConsignmentId:  consignment.ID,
		Amount:         100, // TODO: derive from consignment
		Currency:       "GBP",
		IdempotencyKey: consignment.ID,
	})
	if err != nil {
		log.Printf("failed to authorise payment for consignment %s: %v", consignment.ID, err)
		h.cancelConsignment(ctx, consignment.ID)
		return nil, status.Error(codes.Internal, "failed to authorise payment")
	}

	confirmationEvent := &manager.ConfirmationEvent{
		PaymentAuthID: paymentResponse.AuthId,
		ReservationID: consignment.ReservationID,
		ConsignmentID: consignment.ID,
		VesselID:      consignment.VesselID,
		Weight:        int(consignment.Weight),
		Containers:    len(consignment.Containers),
	}

	eventJSON, err := json.Marshal(confirmationEvent)
	if err != nil {
		// json.Marshal on a struct with primitive fields should never fail,
		// but handle it defensively
		log.Printf("ALERT: failed to marshal confirmation event for consignment %s: %v", consignment.ID, err)
		h.cancelConsignment(ctx, consignment.ID)
		h.voidPayment(ctx, paymentResponse.AuthId)
		return nil, status.Error(codes.Internal, "failed to process confirmation")
	}

	if err = h.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Key:     consignment.ID,
		Topic:   manager.ConsignmentPaymentAuthorisedTopic,
		Payload: eventJSON,
	}); err != nil {
		log.Printf("failed to write outbox event for consignment %s: %v", consignment.ID, err)
		h.cancelConsignment(ctx, consignment.ID)
		h.voidPayment(ctx, paymentResponse.AuthId)
		return nil, status.Error(codes.Internal, "failed to schedule confirmation")
	}

	return &pb.ConfirmResponse{Confirmed: true}, nil
}

func (h *handler) cancelConsignment(ctx context.Context, id string) {
	if err := h.repository.UpdateStatus(ctx, id, storage.StatusCancelled); err != nil {
		// TODO - add alert
		log.Printf("ALERT: failed to cancel consignment %s: %v", id, err)
	}
}

func (h *handler) voidPayment(ctx context.Context, authID string) {
	if _, err := h.paymentCli.Void(ctx, &paymentpb.VoidRequest{AuthId: authID}); err != nil {
		log.Printf("ALERT: failed to void payment auth %s — authorisation will expire naturally: %v", authID, err)
	}
}

func (h *handler) GetConsignments(ctx context.Context, req *pb.GetRequest) (*pb.Response, error) {
	consignments, err := h.repository.GetAll(ctx)
	if err != nil {
		log.Printf("failed to get consignments: %v\n", err)
		return nil, status.Error(codes.Internal, "failed to get consignments")
	}

	return &pb.Response{Consignments: mongo.UnmarshalConsignmentCollection(consignments)}, nil
}

// *** HELPERS ***

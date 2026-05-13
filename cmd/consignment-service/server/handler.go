package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/murraystewart96/shippy/consignment-service/manager"
	"github.com/murraystewart96/shippy/consignment-service/storage"
	"github.com/murraystewart96/shippy/consignment-service/storage/mongo"
	pb "github.com/murraystewart96/shippy/proto/consignment"
	paymentpb "github.com/murraystewart96/shippy/proto/payment"
	reservepb "github.com/murraystewart96/shippy/proto/reservation"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	pb.UnimplementedConsignmentServiceServer
	repository     storage.ConsignmentRepository
	reservationCli reservepb.ReservationServiceClient
	paymentCli     paymentpb.PaymentServiceClient
	outbox         storage.OutboxRepository
}

func NewHandler(
	repo storage.ConsignmentRepository,
	reservationCli reservepb.ReservationServiceClient,
	paymentCli paymentpb.PaymentServiceClient,
	outbox storage.OutboxRepository,
) *Handler {
	return &Handler{
		repository:     repo,
		reservationCli: reservationCli,
		paymentCli:     paymentCli,
		outbox:         outbox,
	}
}

func (h *Handler) CreateConsignment(ctx context.Context, req *pb.Consignment) (*pb.Response, error) {
	consignmentID := uuid.New()
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("consignment_id", consignmentID.String()))

	reservationResponse, err := h.reservationCli.ReserveCapacity(ctx, &reservepb.ReserveCapacityRequest{
		ConsignmentId:      consignmentID.String(),
		Weight:             req.Weight,
		NumberOfContainers: int32(len(req.Containers)),
	})
	if err != nil {
		log.Printf("failed to reserve capacity on vessel: %v\n", err)
		return nil, status.Error(codes.Internal, "failed to reserve capacity")
	}

	req.VesselId = reservationResponse.VesselId

	consignment := mongo.MarshalConsignment(req)
	consignment.ID = consignmentID.String()
	consignment.ReservationID = reservationResponse.Id

	log.Printf("Creating consignment\n")
	if err := h.repository.Create(ctx, consignment); err != nil {
		log.Printf("failed to create consignment: %v\n", err)
		return nil, status.Error(codes.Internal, "failed to create consignment")
	}

	return &pb.Response{Created: true, Consignment: mongo.UnmarshalConsignment(consignment)}, nil
}

func (h *Handler) ConfirmConsignment(ctx context.Context, req *pb.ConfirmRequest) (*pb.ConfirmResponse, error) {
	sagaStartedAt := time.Now()
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("consignment_id", req.Id))

	consignment, err := h.repository.GetByID(ctx, req.Id)
	if err != nil {
		log.Printf("failed to get consignment %s: %v", req.Id, err)
		return nil, status.Error(codes.NotFound, "consignment not found")
	}

	_, err = h.reservationCli.RefreshReservation(ctx, &reservepb.CapacityActionRequest{Id: consignment.ReservationID})
	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.NotFound {
			return nil, status.Error(codes.FailedPrecondition, "reservation no longer exists")
		}
		log.Printf("failed to refresh reservation %s: %v", consignment.ReservationID, err)
		return nil, status.Error(codes.Internal, "failed to refresh reservation")
	}

	paymentResponse, err := h.paymentCli.Authorise(ctx, &paymentpb.AuthoriseRequest{
		ConsignmentId:  consignment.ID,
		Amount:         100,
		Currency:       "GBP",
		IdempotencyKey: consignment.ID,
	})
	if err != nil {
		log.Printf("failed to authorise payment for consignment %s: %v", consignment.ID, err)
		cancelConsignment(ctx, consignment.ID, h.repository)
		return nil, status.Error(codes.Internal, "failed to authorise payment")
	}

	if err := h.publishPaymentAuthorised(ctx, paymentResponse.AuthId, consignment, sagaStartedAt); err != nil {
		//TODO: maybe we should do this - cancelConsignment(ctx, consignment.ID, h.repository)

		return nil, fmt.Errorf("failed to confirm consignment: %w", err)
	}

	return &pb.ConfirmResponse{Confirmed: true}, nil
}

func (h *Handler) GetConsignments(ctx context.Context, _ *pb.GetRequest) (*pb.Response, error) {
	consignments, err := h.repository.GetAll(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to get consignments")
	}
	return &pb.Response{Consignments: mongo.UnmarshalConsignmentCollection(consignments)}, nil
}

func (h *Handler) publishPaymentAuthorised(ctx context.Context, paymentAuthID string, consignment *storage.Consignment, sagaStartedAt time.Time) error {
	confirmationEvent := &manager.ConfirmationEvent{
		PaymentAuthID: paymentAuthID,
		ReservationID: consignment.ReservationID,
		ConsignmentID: consignment.ID,
		VesselID:      consignment.VesselID,
		Weight:        int(consignment.Weight),
		Containers:    len(consignment.Containers),
		SagaStartedAt: sagaStartedAt,
	}

	eventJSON, err := json.Marshal(confirmationEvent)
	if err != nil {
		log.Printf("ALERT: failed to marshal confirmation event for consignment %s: %v", consignment.ID, err)
		cancelConsignment(ctx, consignment.ID, h.repository)
		voidPayment(ctx, paymentAuthID, h.paymentCli)
		return status.Error(codes.Internal, "failed to process confirmation")
	}

	if err = h.outbox.CreateEvent(ctx, &storage.OutboxEvent{
		Key:     consignment.ID,
		Topic:   manager.ConsignmentPaymentAuthorisedTopic,
		Payload: eventJSON,
	}); err != nil {
		log.Printf("failed to write outbox event for consignment %s: %v", consignment.ID, err)
		cancelConsignment(ctx, consignment.ID, h.repository)
		voidPayment(ctx, paymentAuthID, h.paymentCli)
		return status.Error(codes.Internal, "failed to schedule confirmation")
	}

	return nil
}

func cancelConsignment(ctx context.Context, id string, repo storage.ConsignmentRepository) {
	// TODO: MAYBE we should cancel the reservation here instead of purely relying on the expiration
	if err := repo.UpdateStatus(ctx, id, storage.StatusCancelled); err != nil {
		log.Printf("ALERT: failed to cancel consignment %s: %v", id, err)
	}
}

func voidPayment(ctx context.Context, authID string, paymentCli paymentpb.PaymentServiceClient) {
	if _, err := paymentCli.Void(ctx, &paymentpb.VoidRequest{AuthId: authID}); err != nil {
		log.Printf("ALERT: failed to void payment auth %s — authorisation will expire naturally: %v", authID, err)
	}
}

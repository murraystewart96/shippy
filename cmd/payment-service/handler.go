package main

import (
	"context"
	"sync"

	"github.com/google/uuid"
	pb "github.com/murraystewart96/shippy/proto/payment"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type paymentStatus int

const (
	statusAuthorised paymentStatus = iota
	statusCaptured
	statusRefunded
	statusVoided
)

type payment struct {
	id            string
	consignmentID string
	amount        int64
	currency      string
	status        paymentStatus
	captureID     string // set on capture, used as idempotency anchor for refund
}

type handler struct {
	pb.UnimplementedPaymentServiceServer
	mu       sync.RWMutex
	payments map[string]*payment // keyed by auth_id
}

func (h *handler) Authorise(_ context.Context, req *pb.AuthoriseRequest) (*pb.AuthoriseResponse, error) {
	authID := uuid.NewString()

	h.mu.Lock()
	h.payments[authID] = &payment{
		id:            authID,
		consignmentID: req.ConsignmentId,
		amount:        req.Amount,
		currency:      req.Currency,
		status:        statusAuthorised,
	}
	h.mu.Unlock()

	return &pb.AuthoriseResponse{
		AuthId: authID,
		Status: pb.PaymentStatus_AUTHORISED,
	}, nil
}

func (h *handler) Capture(_ context.Context, req *pb.CaptureRequest) (*pb.CaptureResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	p, ok := h.payments[req.AuthId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "authorisation %s not found", req.AuthId)
	}

	// Idempotent — if already captured return the existing capture ID
	if p.status == statusCaptured {
		return &pb.CaptureResponse{
			PaymentId: p.captureID,
			Status:    pb.PaymentStatus_CAPTURED,
		}, nil
	}

	if p.status != statusAuthorised {
		return nil, status.Errorf(codes.FailedPrecondition, "payment is in status %d, cannot capture", p.status)
	}

	p.status = statusCaptured
	p.captureID = req.IdempotencyKey

	return &pb.CaptureResponse{
		PaymentId: p.captureID,
		Status:    pb.PaymentStatus_CAPTURED,
	}, nil
}

func (h *handler) Refund(_ context.Context, req *pb.RefundRequest) (*pb.RefundResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Find payment by captureID (payment_id)
	var p *payment
	for _, v := range h.payments {
		if v.captureID == req.PaymentId {
			p = v
			break
		}
	}
	if p == nil {
		return nil, status.Errorf(codes.NotFound, "payment %s not found", req.PaymentId)
	}

	// Idempotent — already refunded
	if p.status == statusRefunded {
		return &pb.RefundResponse{
			RefundId: req.IdempotencyKey,
			Status:   pb.PaymentStatus_REFUNDED,
		}, nil
	}

	if p.status != statusCaptured {
		return nil, status.Errorf(codes.FailedPrecondition, "payment is in status %d, cannot refund", p.status)
	}

	p.status = statusRefunded

	return &pb.RefundResponse{
		RefundId: req.IdempotencyKey,
		Status:   pb.PaymentStatus_REFUNDED,
	}, nil
}

func (h *handler) Void(_ context.Context, req *pb.VoidRequest) (*pb.VoidResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	p, ok := h.payments[req.AuthId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "authorisation %s not found", req.AuthId)
	}

	// Idempotent — already voided
	if p.status == statusVoided {
		return &pb.VoidResponse{Status: pb.PaymentStatus_VOIDED}, nil
	}

	if p.status != statusAuthorised {
		return nil, status.Errorf(codes.FailedPrecondition, "payment is in status %d, cannot void", p.status)
	}

	p.status = statusVoided

	return &pb.VoidResponse{Status: pb.PaymentStatus_VOIDED}, nil
}

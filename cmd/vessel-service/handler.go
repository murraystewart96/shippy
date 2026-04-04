package main

import (
	"context"
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
	vessel, err := h.repository.ReserveCapacity(ctx, MarshalSpecification(req))
	if err != nil {
		log.Printf("failed to reserve capacity on vessel: %v\n", err)
		return nil, status.Error(codes.NotFound, "no vessel available for specification")
	}

	return &pb.Response{Vessel: UnmarshalVessel(vessel)}, nil
}

func (h *handler) ReleaseCapacity(ctx context.Context, req *pb.CapacityRequest) (*pb.Empty, error) {

	return nil, nil
}

func (h *handler) ConfirmCapacity(ctx context.Context, req *pb.CapacityRequest) (*pb.Empty, error) {

	return nil, nil
}

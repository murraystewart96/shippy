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

func (h *handler) FindAvailable(ctx context.Context, req *pb.Specification) (*pb.Response, error) {
	vessel, err := h.repository.FindAvailable(ctx, MarshalSpecification(req))
	if err != nil {
		log.Printf("failed to find vessel: %v\n", err)
		return nil, status.Error(codes.NotFound, "no vessel available for specification")
	}

	return &pb.Response{Vessel: UnmarshalVessel(vessel)}, nil
}

func (h *handler) Create(ctx context.Context, req *pb.Vessel) (*pb.Response, error) {
	if err := h.repository.Create(ctx, MarshalVessel(req)); err != nil {
		log.Printf("failed to create vessel: %v\n", err)
		return nil, status.Error(codes.Internal, "failed to create vessel")
	}

	return &pb.Response{Created: true, Vessel: req}, nil
}

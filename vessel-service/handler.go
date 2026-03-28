package main

import (
	"context"
	"log"

	pb "github.com/murraystewart96/shippy/proto/vessel"
)

// Our grpc service handler
type handler struct {
	repository repository
}

func (h *handler) FindAvailable(ctx context.Context, req *pb.Specification, res *pb.Response) error {
	// Find the next available vessel
	vessel, err := h.repository.FindAvailable(ctx, MarshalSpecification(req))
	if err != nil {
		log.Printf("failed to find vessel: %v\n", err)

		return err
	}

	// Set the vessel as part of the response message type
	res.Vessel = UnmarshalVessel(vessel)
	return nil
}

func (h *handler) Create(ctx context.Context, req *pb.Vessel, res *pb.Response) error {
	err := h.repository.Create(ctx, MarshalVessel(req))
	if err != nil {
		log.Printf("failed to create vessel: %v\n", err)

		return err
	}

	res.Created = true
	res.Vessel = req

	return nil
}

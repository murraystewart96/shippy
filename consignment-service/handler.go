package main

import (
	"context"
	"fmt"
	"log"

	pb "github.com/murraystewart96/shippy/consignment-service/proto/consignment"
	vesselpb "github.com/murraystewart96/shippy/vessel-service/proto/vessel"
)

// implementation of grpc service
type handler struct {
	repository repository
	vesselCli  vesselpb.VesselService
}

func (h *handler) CreateConsignment(ctx context.Context, req *pb.Consignment, res *pb.Response) error {
	// Find vessel for consignment
	vesselSpec := &vesselpb.Specification{
		Capacity:  int32(len(req.Containers)),
		MaxWeight: req.Weight,
	}

	vesselResponse, err := h.vesselCli.FindAvailable(ctx, vesselSpec)
	if err != nil {
		log.Printf("failed to find vessel: %v\n", err)

		return fmt.Errorf("failed to find vessel: %w", err)
	}

	// Assign vessel to consignment
	req.Id = vesselResponse.Vessel.Id

	// Create consignment
	err = h.repository.Create(ctx, MarshalConsignment(req))
	if err != nil {
		log.Printf("failed to create consignment: %v\n", err)

		return err
	}
	res.Created = true
	res.Consignment = req

	return nil
}

func (h *handler) GetConsignments(ctx context.Context, req *pb.GetRequest, res *pb.Response) error {
	consignments, err := h.repository.GetAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to get consignments: %w", err)
	}

	res.Consignments = UnmarshalConsignmentCollection(consignments)
	return nil
}

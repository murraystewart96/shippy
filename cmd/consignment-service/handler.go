package main

import (
	"context"
	"log"

	pb "github.com/murraystewart96/shippy/proto/consignment"
	reservepb "github.com/murraystewart96/shippy/proto/reservation"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type handler struct {
	pb.UnimplementedConsignmentServiceServer
	repository     repository
	vesselCli      vesselpb.VesselServiceClient
	reservationCli reservepb.ReservationServiceClient
}

func (h *handler) CreateConsignment(ctx context.Context, req *pb.Consignment) (*pb.Response, error) {
	response, err := h.reservationCli.ReserveCapacity(ctx, &reservepb.CapacityInfo{
		Weight:             req.Weight,
		NumberOfContainers: int32(len(req.Containers)),
	})
	if err != nil {
		log.Printf("failed to reserve capacity on vessel: %v\n", err)
		return nil, status.Error(codes.Internal, "failed to reserve capacity")
	}

	req.VesselId = response.VesselId

	c := MarshalConsignment(req)
	if err := h.repository.Create(ctx, c); err != nil {
		log.Printf("failed to create consignment: %v\n", err)
		return nil, status.Error(codes.Internal, "failed to create consignment")
	}

	// Pay

	// Confirm

	return &pb.Response{Created: true, Consignment: UnmarshalConsignment(c)}, nil
}

func (h *handler) GetConsignments(ctx context.Context, req *pb.GetRequest) (*pb.Response, error) {
	consignments, err := h.repository.GetAll(ctx)
	if err != nil {
		log.Printf("failed to get consignments: %v\n", err)
		return nil, status.Error(codes.Internal, "failed to get consignments")
	}

	return &pb.Response{Consignments: UnmarshalConsignmentCollection(consignments)}, nil
}

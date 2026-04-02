package server

import (
	"context"

	pb "github.com/murraystewart96/shippy/proto/reservation"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/storage"
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

func (h *GRPCHandler) ReserveCapacity(ctx context.Context, req *pb.CapacityInfo) (*pb.ReservationResponse, error) {

	// Reserve Capacity on vessel
	// Implement rpc on vessel to find and create reservation

	return nil, nil
}

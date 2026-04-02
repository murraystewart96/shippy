package main

import (
	"github.com/rs/zerolog/log"

	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/config"
	"github.com/murraystewart96/shippy/reservation-service/server"
	"github.com/murraystewart96/shippy/reservation-service/storage/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	cfg := &config.Config{}
	config.ReadEnvironment("", cfg)

	//
	cache := redis.NewCache(&cfg.Redis)

	//
	vesselConn, err := grpc.NewClient(cfg.VesselService.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to vessel service")
	}
	defer vesselConn.Close()

	//
	handler := server.NewGRPCHandler(vesselpb.NewVesselServiceClient(vesselConn), cache)

	grpcServer := server.NewGRPCServer(handler)

	if err := server.GRPCServe(grpcServer, cfg.Address); err != nil {
		log.Fatal().Err(err).Msg("failed start server")
	}

}

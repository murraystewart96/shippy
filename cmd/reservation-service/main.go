package main

import (
	"github.com/rs/zerolog/log"

	"github.com/murraystewart96/shippy/pkg/kafka"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
	"github.com/murraystewart96/shippy/reservation-service/config"
	"github.com/murraystewart96/shippy/reservation-service/manager"
	"github.com/murraystewart96/shippy/reservation-service/server"
	"github.com/murraystewart96/shippy/reservation-service/storage/postgres"
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

	vesselCli := vesselpb.NewVesselServiceClient(vesselConn)

	//
	db, err := postgres.NewDB(&cfg.DB)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to connect to DB")
	}
	producer, err := kafka.NewProducer(&kafka.ProducerConfig{
		BootstrapServers: cfg.KafkaProducer.BootstrapServers,
		Acks:             cfg.KafkaProducer.Acks,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create kafka producer")
	}

	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		BootstrapServers: cfg.KafkaConsumer.BootstrapServers,
		GroupID:          cfg.KafkaConsumer.GroupID,
		OffsetReset:      cfg.KafkaConsumer.OffsetReset,
	})
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create kafka consumer")
	}

	// Create manager
	_, err = manager.New(vesselCli, producer, consumer, nil, cache, db)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create reservation manager")
	}

	//
	handler := server.NewGRPCHandler(vesselCli, cache)

	grpcServer := server.NewGRPCServer(handler)

	if err := server.GRPCServe(grpcServer, cfg.Address); err != nil {
		log.Fatal().Err(err).Msg("failed start server")
	}

}

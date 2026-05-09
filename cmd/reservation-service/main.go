package main

import (
	"context"
	"os/signal"
	"sync"
	"syscall"

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
	if err := run(); err != nil {
		log.Fatal().Err(err).Send()
	}
}

func run() error {
	cfg := &config.Config{}
	config.ReadEnvironment("", cfg)

	cache := redis.NewCache(&cfg.Redis)

	vesselConn, err := grpc.NewClient(cfg.VesselService.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer vesselConn.Close()

	vesselCli := vesselpb.NewVesselServiceClient(vesselConn)

	db, err := postgres.NewDB(&cfg.DB)
	if err != nil {
		return err
	}

	if err := kafka.EnsureTopics(context.Background(), cfg.KafkaProducer.BootstrapServers, []kafka.TopicConfig{
		{Name: manager.PaymentCapturedTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.ReleaseCapacityTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.ConfirmCapacityTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.CapacityFailedTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.ReservationExpiredTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.ConsignmentConfirmationFailedTopic, NumPartitions: 1, ReplicationFactor: 1},
		{Name: manager.ReservationConfirmedTopic, NumPartitions: 1, ReplicationFactor: 1},
	}); err != nil {
		return err
	}

	producer, err := kafka.NewProducer(&kafka.ProducerConfig{
		BootstrapServers: cfg.KafkaProducer.BootstrapServers,
		Acks:             cfg.KafkaProducer.Acks,
	})
	if err != nil {
		return err
	}

	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		BootstrapServers: cfg.KafkaConsumer.BootstrapServers,
		GroupID:          cfg.KafkaConsumer.GroupID,
		OffsetReset:      cfg.KafkaConsumer.OffsetReset,
	})
	if err != nil {
		return err
	}

	mgr, err := manager.New(vesselCli, producer, consumer, []string{
		manager.PaymentCapturedTopic,
		manager.ReleaseCapacityTopic,
		manager.ConfirmCapacityTopic,
		manager.ReservationExpiredTopic,
		manager.CapacityFailedTopic,
	}, cache, db, cfg.Manager)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	handler := server.NewGRPCHandler(vesselCli, cache)
	grpcServer := server.NewGRPCServer(handler)

	var wg sync.WaitGroup
	managerErrCh := mgr.Start(ctx, &wg)
	grpcErrCh := server.GRPCServe(grpcServer, cfg.Address, &wg)

	select {
	case err := <-managerErrCh:
		log.Error().Err(err).Msg("manager error — shutting down")
		cancel()
	case err := <-grpcErrCh:
		log.Error().Err(err).Msg("gRPC server error — shutting down")
		cancel()
	case <-ctx.Done():
	}

	grpcServer.GracefulStop()
	wg.Wait()

	return nil
}

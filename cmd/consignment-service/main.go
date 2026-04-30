package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/rs/zerolog/log"

	"github.com/murraystewart96/shippy/consignment-service/manager"
	"github.com/murraystewart96/shippy/consignment-service/server"
	"github.com/murraystewart96/shippy/consignment-service/storage/mongo"
	"github.com/murraystewart96/shippy/pkg/kafka"
	paymentpb "github.com/murraystewart96/shippy/proto/payment"
	reservepb "github.com/murraystewart96/shippy/proto/reservation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultHost = "consignment-service-database"
	defaultPort = "27017"
)

func main() {
	if err := run(); err != nil {
		log.Fatal().Err(err).Send()
	}
}

func run() error {
	grpcAddr := os.Getenv("GRPC_ADDRESS")
	if grpcAddr == "" {
		grpcAddr = ":50051"
	}

	reservationServiceAddr := os.Getenv("RESERVATION_SERVICE_ADDRESS")
	if reservationServiceAddr == "" {
		reservationServiceAddr = "localhost:50054"
	}

	paymentServiceAddr := os.Getenv("PAYMENT_SERVICE_ADDRESS")
	if paymentServiceAddr == "" {
		paymentServiceAddr = "localhost:50055"
	}

	kafkaAddr := os.Getenv("KAFKA_ADDRESS")
	if kafkaAddr == "" {
		kafkaAddr = "localhost:9092"
	}

	outboxInterval := 30
	if v := os.Getenv("OUTBOX_INTERVAL"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			outboxInterval = n
		}
	}

	host := os.Getenv("DB_HOST")
	if host == "" {
		host = defaultHost
	}
	port := os.Getenv("DB_PORT")
	if port == "" {
		port = defaultPort
	}

	mongoCli, err := mongo.CreateClient(context.Background(), fmt.Sprintf("mongodb://%s:%s", host, port), 0)
	if err != nil {
		return err
	}
	defer mongoCli.Disconnect(context.Background())

	repository := mongo.New(mongoCli.Database("shippy").Collection("consignments"))
	outbox := mongo.NewOutbox(mongoCli.Database("shippy").Collection("outbox"))

	reservationConn, err := grpc.NewClient(reservationServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer reservationConn.Close()

	paymentConn, err := grpc.NewClient(paymentServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer paymentConn.Close()

	paymentCli := paymentpb.NewPaymentServiceClient(paymentConn)

	producer, err := kafka.NewProducer(&kafka.ProducerConfig{
		BootstrapServers: kafkaAddr,
		Acks:             "all",
	})
	if err != nil {
		return err
	}

	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		BootstrapServers: kafkaAddr,
		GroupID:          "consignment-service",
		OffsetReset:      "earliest",
	})
	if err != nil {
		return err
	}

	mgr, err := manager.New(
		producer,
		consumer,
		[]string{
			manager.ConsignmentPaymentAuthorisedTopic,
			manager.ConsignmentConfirmationFailedTopic,
			manager.ReservationExpiredTopic,
		},
		outbox,
		paymentCli,
		repository,
		manager.Config{OutboxInterval: outboxInterval},
	)
	if err != nil {
		return err
	}

	h := server.NewHandler(repository, reservepb.NewReservationServiceClient(reservationConn), paymentCli, outbox)
	grpcServer := server.NewGRPCServer(h)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	managerErrCh := mgr.Start(ctx, &wg)
	grpcErrCh := server.GRPCServe(grpcServer, grpcAddr, &wg)

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

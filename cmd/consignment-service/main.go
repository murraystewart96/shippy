package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	consignmentmanager "github.com/murraystewart96/shippy/consignment-service/manager"
	"github.com/murraystewart96/shippy/consignment-service/storage/mongo"
	"github.com/murraystewart96/shippy/pkg/kafka"
	pb "github.com/murraystewart96/shippy/proto/consignment"
	paymentpb "github.com/murraystewart96/shippy/proto/payment"
	reservepb "github.com/murraystewart96/shippy/proto/reservation"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
)

const (
	defaultHost = "consignment-service-database"
	defaultPort = "27017"
)

func main() {
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
	uri := fmt.Sprintf("mongodb://%s:%s", host, port)

	mongoCli, err := mongo.CreateClient(context.Background(), uri, 0)
	if err != nil {
		log.Panic(err)
	}
	defer mongoCli.Disconnect(context.Background())

	consignmentCollection := mongoCli.Database("shippy").Collection("consignments")
	outboxCollection := mongoCli.Database("shippy").Collection("outbox")

	repository := mongo.New(consignmentCollection)
	outbox := mongo.NewOutbox(outboxCollection)

	reservationConn, err := grpc.NewClient(reservationServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to reservation service: %v", err)
	}
	defer reservationConn.Close()

	paymentConn, err := grpc.NewClient(paymentServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to payment service: %v", err)
	}
	defer paymentConn.Close()

	paymentCli := paymentpb.NewPaymentServiceClient(paymentConn)

	producer, err := kafka.NewProducer(&kafka.ProducerConfig{
		BootstrapServers: kafkaAddr,
		Acks:             "all",
	})
	if err != nil {
		log.Fatalf("failed to create kafka producer: %v", err)
	}

	consumer, err := kafka.NewConsumer(&kafka.ConsumerConfig{
		BootstrapServers: kafkaAddr,
		GroupID:          "consignment-service",
		OffsetReset:      "earliest",
	})
	if err != nil {
		log.Fatalf("failed to create kafka consumer: %v", err)
	}

	mgr, err := consignmentmanager.New(
		producer,
		consumer,
		[]string{
			consignmentmanager.ConsignmentPaymentAuthorisedTopic,
			consignmentmanager.ConsignmentConfirmationFailedTopic,
			consignmentmanager.ConsignmentConfirmedTopic,
			consignmentmanager.ConsignmentCancelledTopic,
			consignmentmanager.ConsignmentStatusFailedTopic,
		},
		outbox,
		paymentCli,
		repository,
		consignmentmanager.Config{OutboxInterval: outboxInterval},
	)
	if err != nil {
		log.Fatalf("failed to create manager: %v", err)
	}

	handler := &handler{
		repository:     repository,
		reservationCli: reservepb.NewReservationServiceClient(reservationConn),
		paymentCli:     paymentCli,
		outbox:         outbox,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	errCh := mgr.Start(ctx, &wg)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterConsignmentServiceServer(srv, handler)
	reflection.Register(srv)

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		select {
		case <-quit:
			log.Println("shutting down...")
		case err := <-errCh:
			log.Printf("manager error: %v", err)
		}
		cancel()
		srv.GracefulStop()
	}()

	log.Printf("consignment-service listening on %s", grpcAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	wg.Wait()
}

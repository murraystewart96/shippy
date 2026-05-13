package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/google/uuid"
	pb "github.com/murraystewart96/shippy/proto/vessel"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	defaultHost = "vessel-service-database"
	defaultPort = "27017"
)

func main() {
	grpcAddr := os.Getenv("GRPC_ADDRESS")
	if grpcAddr == "" {
		grpcAddr = ":50051"
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

	mongoCli, err := CreateMongoClient(context.Background(), uri, 0)
	if err != nil {
		log.Panic(err)
	}
	defer mongoCli.Disconnect(context.Background())

	vesselCollection := mongoCli.Database("shippy").Collection("vessels")
	capacityOpsCollection := mongoCli.Database("shippy").Collection("capacity_operations")
	repo := &MongoRepository{
		client:      mongoCli,
		vessels:     vesselCollection,
		capacityOps: capacityOpsCollection,
	}

	if err := repo.SetupIndexes(context.Background()); err != nil {
		log.Panicf("failed to setup indexes: %v", err)
	}

	for _, v := range []*Vessel{
		{ID: uuid.NewString(), Name: "Boaty McBoatface", MaxWeight: 10000000, Capacity: 100000},
		{ID: uuid.NewString(), Name: "The Unsinkable II", MaxWeight: 10000000, Capacity: 100000},
		{ID: uuid.NewString(), Name: "SS Load Test", MaxWeight: 10000000, Capacity: 100000},
	} {
		if err := repo.Create(context.Background(), v); err != nil {
			log.Printf("seed vessel: %v", err)
		}
	}

	shutdown, err := initTracer(context.Background(), "gateway")
	if err != nil {
		log.Fatal("failed to init tracer: %w", err)
	}
	defer shutdown()

	handler := &handler{repository: repo}

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	pb.RegisterVesselServiceServer(srv, handler)
	reflection.Register(srv)

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		log.Println("shutting down...")
		srv.GracefulStop()
	}()

	log.Printf("vessel-service listening on %s", grpcAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func initTracer(ctx context.Context, serviceName string) (func(), error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(exporter),
		trace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName),
		)),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return func() { _ = tp.Shutdown(ctx) }, nil
}

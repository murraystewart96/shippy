package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"

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

	metricsAddr := os.Getenv("METRICS_ADDRESS")
	if metricsAddr == "" {
		metricsAddr = ":9090"
	}
	go serveMetrics(metricsAddr)

	shutdown, err := initTracer(context.Background(), "reservation")
	if err != nil {
		return fmt.Errorf("failed to init tracer: %w", err)
	}
	defer shutdown()

	cache := redis.NewCache(&cfg.Redis)

	vesselConn, err := grpc.NewClient(cfg.VesselService.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
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

func serveMetrics(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal().Err(err).Msg("metrics server failed")
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

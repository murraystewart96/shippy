package main

import (
	"context"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/murraystewart96/shippy/cmd/gateway/handler"
	"github.com/murraystewart96/shippy/cmd/gateway/middleware"
	consignmentpb "github.com/murraystewart96/shippy/proto/consignment"
	userpb "github.com/murraystewart96/shippy/proto/user"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	serverAddr := os.Getenv("SERVER_ADDRESS")
	if serverAddr == "" {
		serverAddr = "localhost:50053"
	}

	userServiceAddr := os.Getenv("USER_SERVICE_ADDRESS")
	if userServiceAddr == "" {
		userServiceAddr = "localhost:50053"
	}

	consignmentServiceAddr := os.Getenv("CONSIGNMENT_SERVICE_ADDRESS")
	if consignmentServiceAddr == "" {
		consignmentServiceAddr = "localhost:50052"
	}

	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		log.Fatal("no JWT secret")
	}

	shutdown, err := initTracer(context.Background(), "gateway")
	if err != nil {
		log.Fatal("failed to init tracer: %w", err)
	}
	defer shutdown()

	// Init user and consignment grpc clients
	userConn, err := grpc.NewClient(userServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalf("failed to connect to user service: %v", err)
	}
	defer userConn.Close()

	consignmentConn, err := grpc.NewClient(consignmentServiceAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalf("failed to connect to vessel service: %v", err)
	}
	defer consignmentConn.Close()

	h := handler.New(
		userpb.NewUserServiceClient(userConn),
		consignmentpb.NewConsignmentServiceClient(consignmentConn),
	)

	// Create a Gin router with default middleware (logger and recovery)
	r := gin.Default()

	r.Use(otelgin.Middleware("gateway"))

	// Assign handlers
	r.POST("/auth", h.Auth)
	r.POST("/v1/users", h.CreateUser)

	v1 := r.Group("/v1", middleware.Auth(jwtSecret))
	{
		v1.POST("/consignments", h.CreateConsignment)
		v1.POST("/consignments/confirm/:id", h.ConfirmConsignment)
		v1.GET("/consignments", h.GetConsignments)
	}

	// Start server on port 8080 (default)
	// Server will listen on 0.0.0.0:8080
	if err := r.Run(serverAddr); err != nil {
		log.Fatalf("failed to run server: %v", err)
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

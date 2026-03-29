package main

import (
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/murraystewart96/shippy/cmd/gateway/handler"
	"github.com/murraystewart96/shippy/cmd/gateway/middleware"
	consignmentpb "github.com/murraystewart96/shippy/proto/consignment"
	userpb "github.com/murraystewart96/shippy/proto/user"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
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

	// Init user and consignment grpc clients
	userConn, err := grpc.NewClient(userServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to user service: %v", err)
	}
	defer userConn.Close()

	consignmentConn, err := grpc.NewClient(consignmentServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

	// Assign handlers
	r.POST("/auth", h.Auth)
	r.POST("/v1/users", h.CreateUser)

	v1 := r.Group("/v1", middleware.Auth(jwtSecret))
	{
		v1.POST("/consignments", h.CreateConsignment)
		v1.GET("/consignments", h.GetConsignments)
	}

	// Start server on port 8080 (default)
	// Server will listen on 0.0.0.0:8080 (localhost:8080 on Windows)
	if err := r.Run(); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}

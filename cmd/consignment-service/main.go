package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/murraystewart96/shippy/proto/consignment"
	vesselpb "github.com/murraystewart96/shippy/proto/vessel"
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

	userServiceAddr := os.Getenv("USER_SERVICE_ADDRESS")
	if userServiceAddr == "" {
		userServiceAddr = "localhost:50053"
	}

	vesselServiceAddr := os.Getenv("VESSEL_SERVICE_ADDRESS")
	if vesselServiceAddr == "" {
		vesselServiceAddr = "localhost:50052"
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

	consignmentCollection := mongoCli.Database("shippy").Collection("consignments")
	repository := &MongoRespository{collection: consignmentCollection}

	userConn, err := grpc.NewClient(userServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to user service: %v", err)
	}
	defer userConn.Close()

	vesselConn, err := grpc.NewClient(vesselServiceAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to vessel service: %v", err)
	}
	defer vesselConn.Close()

	handler := &handler{
		repository: repository,
		vesselCli:  vesselpb.NewVesselServiceClient(vesselConn),
	}

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
		<-quit
		log.Println("shutting down...")
		srv.GracefulStop()
	}()

	log.Printf("consignment-service listening on %s", grpcAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

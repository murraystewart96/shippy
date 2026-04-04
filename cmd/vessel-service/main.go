package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/murraystewart96/shippy/proto/vessel"
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

	// Seed a vessel if none exist
	if err := repo.Create(context.Background(), &Vessel{
		ID:        "vessel001",
		Name:      "Boaty McBoatface",
		MaxWeight: 200000,
		Capacity:  500,
	}); err != nil {
		log.Printf("seed vessel: %v", err)
	}

	handler := &handler{repository: repo}

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
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

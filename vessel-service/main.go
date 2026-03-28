package main

import (
	"context"
	"log"
	"os"

	grpcServer "github.com/go-micro/plugins/v4/server/grpc"
	pb "github.com/murraystewart96/shippy/proto/vessel"
	micro "go-micro.dev/v4"
	"go-micro.dev/v4/server"
)

const (
	defaultHost = "datastore:27017"
)

func main() {
	service := micro.NewService(
		micro.Name("shipping.VesselService"),
		micro.Server(grpcServer.NewServer(server.Name("shipping.VesselService"))),
	)

	service.Init()

	uri := os.Getenv("DB_HOST")
	if uri == "" {
		uri = defaultHost
	}

	// Create db client
	mongoCli, err := CreateMongoClient(context.Background(), uri, 0)
	if err != nil {
		log.Panic(err)
	}
	defer mongoCli.Disconnect(context.Background())

	// Configure db
	vesselCollection := mongoCli.Database("shippy").Collection("vessels")

	repo := &MongoRespository{collection: vesselCollection}
	err = repo.Create(context.Background(), &Vessel{
		ID:        "vessel001",
		Name:      "Boaty McBoatface",
		MaxWeight: 200000,
		Capacity:  500,
	})
	if err != nil {
		log.Fatal(err)
	}

	handler := &handler{
		repository: repo,
	}

	// Register our implementation with
	if err := pb.RegisterVesselServiceHandler(service.Server(), handler); err != nil {
		log.Panic(err)
	}

	if err := service.Run(); err != nil {
		log.Panic(err)
	}
}

package main

import (
	"context"
	"log"
	"os"

	grpcClient "github.com/go-micro/plugins/v4/client/grpc"
	grpcServer "github.com/go-micro/plugins/v4/server/grpc"
	pb "github.com/murraystewart96/shippy/shippy-consignment-service/proto/consignment"
	vesselpb "github.com/murraystewart96/shippy/shippy-vessel-service/proto/vessel"

	micro "go-micro.dev/v4"
	"go-micro.dev/v4/server"
)

const (
	defaultHost = "datastore:27017"
)

func main() {
	service := micro.NewService(
		micro.Name("shipping.ConsignmentService"),
		micro.Server(grpcServer.NewServer(
			server.Name("shipping.ConsignmentService"),
		)),
		micro.Client(grpcClient.NewClient()),
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
	consignmentCollection := mongoCli.Database("shippy").Collection("consignments")

	repository := &MongoRespository{
		collection: consignmentCollection,
	}

	// Create vessel service client
	vCli := vesselpb.NewVesselService("shipping.VesselService", service.Client())

	// Create handler
	handler := &handler{
		repository: repository,
		vesselCli:  vCli,
	}

	// Register handler
	if err := pb.RegisterConsignmentServiceHandler(service.Server(), handler); err != nil {
		log.Panic(err)
	}

	if err := service.Run(); err != nil {
		log.Panic(err)
	}
}

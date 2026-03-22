package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"os"

	grpcClient "github.com/go-micro/plugins/v4/client/grpc"
	pb "github.com/murraystewart96/shippy/shippy-consignment-service/proto/consignment"
	micro "go-micro.dev/v4"
)

const (
	defaultFilename = "consignment.json"
)

func parseFile(file string) (*pb.Consignment, error) {
	var consignment *pb.Consignment
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, &consignment)
	return consignment, err
}

func main() {
	service := micro.NewService(
		micro.Name("shipping.ConsignmentCli"),
		micro.Client(grpcClient.NewClient()),
	)
	service.Init()

	client := pb.NewConsignmentService("shipping.ConsignmentService", service.Client())

	file := defaultFilename
	if len(os.Args) > 1 {
		file = os.Args[1]
	}

	consignment, err := parseFile(file)
	if err != nil {
		log.Fatalf("Failed to parse file: %v", err)
	}

	res, err := client.CreateConsignment(context.Background(), consignment)
	if err != nil {
		log.Fatalf("CreateConsigment RPC failed: %v", err)
	}

	log.Printf("Created: %t", res.Created)

	res, err = client.GetConsignments(context.Background(), nil)
	if err != nil {
		log.Fatalf("GetConsignments RPC failed: %v", err)
	}

	for _, v := range res.Consignments {
		log.Println(v)
	}
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"

	grpcClient "github.com/go-micro/plugins/v4/client/grpc"
	pb "github.com/murraystewart96/shippy/proto/consignment"
	micro "go-micro.dev/v4"
	"go-micro.dev/v4/metadata"
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

	// Contact the server and print out its response.
	file := defaultFilename
	var token string
	log.Println(os.Args)

	if len(os.Args) < 3 {
		log.Fatal(errors.New("Not enough arguments, expecing file and token."))
	}

	file = os.Args[1]
	token = os.Args[2]

	consignment, err := parseFile(file)

	if err != nil {
		log.Fatalf("Could not parse file: %v", err)
	}

	// Create a new context which contains our given token.
	// This same context will be passed into both the calls we make
	// to our consignment-service.
	ctx := metadata.NewContext(context.Background(), map[string]string{
		"token": token,
	})

	res, err := client.CreateConsignment(ctx, consignment)
	if err != nil {
		log.Fatalf("CreateConsigment RPC failed: %v", err)
	}

	log.Printf("Created: %t", res.Created)

	res, err = client.GetConsignments(ctx, nil)
	if err != nil {
		log.Fatalf("GetConsignments RPC failed: %v", err)
	}

	for _, v := range res.Consignments {
		log.Println(v)
	}
}

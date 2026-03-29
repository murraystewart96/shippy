package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"

	pb "github.com/murraystewart96/shippy/proto/consignment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const defaultFilename = "consignment.json"

func parseFile(file string) (*pb.Consignment, error) {
	var consignment *pb.Consignment
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(data, &consignment)
	return consignment, err
}

func main() {
	addr := os.Getenv("SERVICE_ADDRESS")
	if addr == "" {
		addr = "localhost:50051"
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewConsignmentServiceClient(conn)

	if len(os.Args) < 3 {
		log.Fatal(errors.New("not enough arguments, expecting file and token"))
	}

	file := os.Args[1]
	token := os.Args[2]

	consignment, err := parseFile(file)
	if err != nil {
		log.Fatalf("Could not parse file: %v", err)
	}

	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("token", token))

	res, err := client.CreateConsignment(ctx, consignment)
	if err != nil {
		log.Fatalf("CreateConsignment RPC failed: %v", err)
	}
	log.Printf("Created: %t", res.Created)

	res, err = client.GetConsignments(ctx, &pb.GetRequest{})
	if err != nil {
		log.Fatalf("GetConsignments RPC failed: %v", err)
	}

	for _, v := range res.Consignments {
		log.Println(v)
	}
}

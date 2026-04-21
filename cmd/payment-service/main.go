package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/murraystewart96/shippy/proto/payment"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	grpcAddr := os.Getenv("GRPC_ADDRESS")
	if grpcAddr == "" {
		grpcAddr = ":50051"
	}

	h := &handler{
		payments: make(map[string]*payment),
	}

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterPaymentServiceServer(srv, h)
	reflection.Register(srv)

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		log.Println("shutting down...")
		srv.GracefulStop()
	}()

	log.Printf("payment-service listening on %s", grpcAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

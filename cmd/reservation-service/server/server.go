package server

import (
	"fmt"
	"log"
	"net"

	pb "github.com/murraystewart96/shippy/proto/reservation"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func NewGRPCServer(handler *GRPCHandler, grpcOpts ...grpc.ServerOption) *grpc.Server {
	//hsrv := health.NewServer()
	//hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	//healthpb.RegisterHealthServer(gsrv, hsrv)

	srv := grpc.NewServer()

	pb.RegisterReservationServiceServer(srv, handler)

	// For testing and debugging
	reflection.Register(srv)

	return srv
}

func GRPCServe(server *grpc.Server, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	log.Printf("reservation-service listening on %s", addr)
	if err := server.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}

	return nil
}

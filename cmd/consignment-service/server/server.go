package server

import (
	"net"
	"sync"

	pb "github.com/murraystewart96/shippy/proto/consignment"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func NewGRPCServer(handler *Handler) *grpc.Server {
	srv := grpc.NewServer()
	pb.RegisterConsignmentServiceServer(srv, handler)
	reflection.Register(srv)
	return srv
}

func GRPCServe(grpcServer *grpc.Server, addr string, wg *sync.WaitGroup) <-chan error {
	errCh := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			errCh <- err
			return
		}
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- err
		}
	}()

	return errCh
}

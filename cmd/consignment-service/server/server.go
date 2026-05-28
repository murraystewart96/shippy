package server

import (
	"net"
	"sync"

	pb "github.com/murraystewart96/shippy/proto/consignment"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func NewGRPCServer(handler *Handler) *grpc.Server {
	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)
	pb.RegisterConsignmentServiceServer(srv, handler)

	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hsrv)

	reflection.Register(srv)
	return srv
}

func GRPCServe(grpcServer *grpc.Server, addr string, wg *sync.WaitGroup) <-chan error {
	errCh := make(chan error, 1)

	wg.Go(func() {
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			errCh <- err
			return
		}
		if err := grpcServer.Serve(lis); err != nil {
			errCh <- err
		}
	})

	return errCh
}

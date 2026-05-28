package server

import (
	"net"
	"sync"

	pb "github.com/murraystewart96/shippy/proto/reservation"
	"github.com/rs/zerolog/log"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
)

func NewGRPCServer(handler *GRPCHandler, grpcOpts ...grpc.ServerOption) *grpc.Server {
	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
	)

	pb.RegisterReservationServiceServer(srv, handler)

	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, hsrv)

	reflection.Register(srv)

	return srv
}

func GRPCServe(server *grpc.Server, addr string, wg *sync.WaitGroup) <-chan error {
	errCh := make(chan error, 1)

	wg.Go(func() {
		lis, err := net.Listen("tcp", addr)
		if err != nil {
			errCh <- err
			return
		}

		log.Info().Str("addr", addr).Msg("reservation-service listening")
		if err := server.Serve(lis); err != nil {
			errCh <- err
		}
	})

	return errCh
}

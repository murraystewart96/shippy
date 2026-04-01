package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/murraystewart96/shippy/proto/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	defaultHost     = ""
	defaultName     = "shippy"
	defaultPassword = "password"
	defaultUser     = "user"
)

func main() {
	grpcAddr := os.Getenv("GRPC_ADDRESS")
	if grpcAddr == "" {
		grpcAddr = ":50051"
	}

	jwtSecret := os.Getenv("JWT_SECRET")
	if jwtSecret == "" {
		log.Fatal("no JWT secret")
	}

	host := os.Getenv("DB_HOST")
	if host == "" {
		host = defaultHost
	}
	user := os.Getenv("DB_USER")
	if user == "" {
		user = defaultUser
	}
	password := os.Getenv("DB_PASSWORD")
	if password == "" {
		password = defaultPassword
	}
	name := os.Getenv("DB_NAME")
	if name == "" {
		name = defaultName
	}

	db, err := CreatePostgresClient(host, user, password, name, 0)
	if err != nil {
		log.Panic(err)
	}
	defer db.Close()

	repository := NewPostgresRepository(db)
	tokenService := &TokenService{
		jwtSecret: jwtSecret,
	}

	handler := &handler{
		repository:   repository,
		tokenService: tokenService,
	}

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := grpc.NewServer()
	pb.RegisterUserServiceServer(srv, handler)
	reflection.Register(srv)

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		log.Println("shutting down...")
		srv.GracefulStop()
	}()

	log.Printf("user-service listening on %s", grpcAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

package main

import (
	"log"
	"os"

	grpcServer "github.com/go-micro/plugins/v4/server/grpc"
	pb "github.com/murraystewart96/shippy/shippy-user-service/proto/user"
	micro "go-micro.dev/v4"
	"go-micro.dev/v4/server"
)

const (
	defaultHost     = ""
	defaultName     = "shippy"
	defaultPassword = "password"
	defaultUser     = "user"
)

func main() {
	service := micro.NewService(
		micro.Name("shipping.UserService"),
		micro.Server(grpcServer.NewServer(server.Name("shipping.UserService"))),
	)

	service.Init()

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

	// Create db client
	db, err := CreatePostgresClient(host, user, password, name, 0)
	if err != nil {
		log.Panic(err)
	}
	defer db.Close()

	repository := NewPostgresRepository(db)

	handler := &handler{
		repository: repository,
	}

	// Register our implementation with
	if err := pb.RegisterUserServiceHandler(service.Server(), handler); err != nil {
		log.Panic(err)
	}

	if err := service.Run(); err != nil {
		log.Panic(err)
	}
}

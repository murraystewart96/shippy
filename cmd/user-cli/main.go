package main

import (
	"context"
	"log"
	"os"

	"github.com/urfave/cli/v2"
	pb "github.com/murraystewart96/shippy/proto/user"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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

	client := pb.NewUserServiceClient(conn)

	var (
		name     string
		email    string
		password string
		company  string
	)

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "name", Usage: "Full name", Destination: &name},
			&cli.StringFlag{Name: "email", Usage: "Email address", Destination: &email},
			&cli.StringFlag{Name: "password", Usage: "Password", Destination: &password},
			&cli.StringFlag{Name: "company", Usage: "Company", Destination: &company},
		},
		Action: func(c *cli.Context) error {
			ctx := context.Background()

			res, err := client.Create(ctx, &pb.User{
				Name:     name,
				Email:    email,
				Company:  company,
				Password: password,
			})
			if err != nil {
				return err
			}
			log.Printf("Created: %s", res.User.Id)

			all, err := client.GetAll(ctx, &pb.Request{})
			if err != nil {
				log.Fatalf("Could not list users: %v", err)
			}

			for _, user := range all.Users {
				log.Println(user)

				authResponse, err := client.Auth(ctx, &pb.User{
					Email:    user.Email,
					Password: password,
				})
				if err != nil {
					log.Fatalf("Could not authenticate user: %s error: %v\n", email, err)
				}

				log.Printf("Your access token is: %s \n", authResponse.Token)
			}

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

package main

import (
	"context"
	"log"

	"github.com/urfave/cli/v2"

	grpcClient "github.com/go-micro/plugins/v4/client/grpc"
	pb "github.com/murraystewart96/shippy/user-service/proto/user"
	micro "go-micro.dev/v4"
)

func main() {
	service := micro.NewService(
		micro.Name("shipping.UserCli"),
		micro.Client(grpcClient.NewClient()),
	)

	client := pb.NewUserService("shipping.UserService", service.Client())

	var (
		name     string
		email    string
		password string
		company  string
	)

	service.Init(
		micro.Flags(
			&cli.StringFlag{Name: "name", Usage: "Full name"},
			&cli.StringFlag{Name: "email", Usage: "Email address"},
			&cli.StringFlag{Name: "password", Usage: "Password"},
			&cli.StringFlag{Name: "company", Usage: "Company"},
		),
		micro.Action(func(c *cli.Context) error {
			name = c.String("name")
			email = c.String("email")
			company = c.String("company")
			password = c.String("password")

			ctx := context.Background()
			user := &pb.User{
				Name:     name,
				Email:    email,
				Company:  company,
				Password: password,
			}

			res, err := client.Create(ctx, user)
			if err != nil {
				return err
			}

			log.Printf("Created: %s", res.User.Id)

			return nil
		}),
	)

	all, err := client.GetAll(context.Background(), &pb.Request{})
	if err != nil {
		log.Fatalf("Could not list users: %v", err)
	}

	for _, user := range all.Users {
		log.Println(user)

		authResponse, err := client.Auth(context.Background(), &pb.User{
			Email:    user.Email,
			Password: password,
		})

		if err != nil {
			log.Fatalf("Could not authenticate user: %s error: %v\n", email, err)
		}

		log.Printf("Your access token is: %s \n", authResponse.Token)
	}

}

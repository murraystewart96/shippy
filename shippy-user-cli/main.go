package main

import (
	"context"

	"github.com/rs/zerolog/log"
	"github.com/urfave/cli/v2"

	grpcClient "github.com/go-micro/plugins/v4/client/grpc"
	pb "github.com/murraystewart96/shippy/shippy-user-service/proto/user"
	micro "go-micro.dev/v4"
)

func createUser(ctx context.Context, service micro.Service, user *pb.User) error {
	client := pb.NewUserService("shipping.UserService", service.Client())

	res, err := client.Create(ctx, user)
	if err != nil {
		return err
	}

	log.Info().Msgf("Response: %v", res.User)

	return nil
}

func main() {
	service := micro.NewService(
		micro.Name("shipping.UserCli"),
		micro.Client(grpcClient.NewClient()),
	)

	service.Init(
		micro.Flags(
			&cli.StringFlag{Name: "name", Usage: "Full name"},
			&cli.StringFlag{Name: "email", Usage: "Email address"},
			&cli.StringFlag{Name: "password", Usage: "Password"},
			&cli.StringFlag{Name: "company", Usage: "Company"},
		),
		micro.Action(func(c *cli.Context) error {
			name := c.String("name")
			email := c.String("email")
			company := c.String("company")
			password := c.String("password")

			ctx := context.Background()
			user := &pb.User{
				Name:     name,
				Email:    email,
				Company:  company,
				Password: password,
			}

			if err := createUser(ctx, service, user); err != nil {
				log.Error().Err(err).Msg("error creating user")
				return err
			}

			return nil
		}),
	)

}

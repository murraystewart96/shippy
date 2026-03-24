package main

import (
	"context"
	"errors"
	"log"
	"os"

	userpb "github.com/murraystewart96/shippy/shippy-user-service/proto/user"

	"go-micro.dev/v4/client"
	"go-micro.dev/v4/metadata"
	"go-micro.dev/v4/server"
)

func AuthWrapper(c client.Client) server.HandlerWrapper {
	return func(fn server.HandlerFunc) server.HandlerFunc {
		return func(ctx context.Context, req server.Request, resp interface{}) error {
			if os.Getenv("DISABLE_AUTH") == "true" {
				return fn(ctx, req, resp)
			}

			meta, ok := metadata.FromContext(ctx)
			if !ok {
				return errors.New("no auth meta-data found in request")
			}

			// Note this is now uppercase (not entirely sure why this is...)
			token := meta["Token"]
			log.Println("Authenticating with token: ", token)

			authClient := userpb.NewUserService("shipping.UserService", c)
			_, err := authClient.ValidateToken(context.Background(), &userpb.Token{
				Token: token,
			})
			if err != nil {
				log.Printf("failed to authenticate user: %v\n", err)
				return err
			}
			return fn(ctx, req, resp)
		}
	}
}

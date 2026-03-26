package main

import (
	"log"

	_ "github.com/go-micro/plugins/v4/broker/nats"
	userpb "github.com/murraystewart96/shippy/user-service/proto/user"
	micro "go-micro.dev/v4"
	"go-micro.dev/v4/broker"
	"google.golang.org/protobuf/encoding/protojson"
)

const topic = "user.created"

func main() {
	srv := micro.NewService(
		micro.Name("shipping.EmailService"),
		micro.Version("latest"),
	)

	srv.Init()

	// Get the broker instance using our environment variables
	pubsub := srv.Server().Options().Broker
	if err := pubsub.Connect(); err != nil {
		log.Fatal(err)
	}

	// Subscribe to messages on the broker
	_, err := pubsub.Subscribe(topic, func(p broker.Event) error {
		var user userpb.User
		if err := protojson.Unmarshal(p.Message().Body, &user); err != nil {
			return err
		}
		log.Println(user.Name)
		go sendEmail(&user)
		return nil
	})

	if err != nil {
		log.Println(err)
	}

	// Run the server
	if err := srv.Run(); err != nil {
		log.Println(err)
	}
}

func sendEmail(user *userpb.User) error {
	log.Println("Sending email to:", user.Name)
	return nil
}

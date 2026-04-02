package main

import (
	"log"

	userpb "github.com/murraystewart96/shippy/proto/user"
)

// TODO: replace with Kafka consumer subscribing to user.created topic
func main() {
	log.Println("email service starting — awaiting Kafka integration")
}

func sendEmail(user *userpb.User) error {
	log.Println("Sending email to:", user.Name)
	return nil
}

package main

import (
	"context"
	"errors"
	"log"

	pb "github.com/murraystewart96/shippy/user-service/proto/user"
	"go-micro.dev/v4/broker"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/protobuf/encoding/protojson"
)

const topic = "user.created"

type authable interface {
	Decode(token string) (*CustomClaims, error)
	Encode(user *pb.User) (string, error)
}

type handler struct {
	repository   Repository
	tokenService authable
	PubSub       broker.Broker
}

func (h *handler) Get(ctx context.Context, req *pb.User, res *pb.Response) error {
	result, err := h.repository.Get(ctx, req.Id)
	if err != nil {
		return err
	}

	user := UnmarshalUser(result)
	res.User = user

	return nil
}

func (h *handler) GetAll(ctx context.Context, req *pb.Request, res *pb.Response) error {
	results, err := h.repository.GetAll(ctx)
	if err != nil {
		return err
	}

	users := UnmarshalUserCollection(results)
	res.Users = users

	return nil
}

func (h *handler) Auth(ctx context.Context, req *pb.User, res *pb.Token) error {
	// Get hashed user password
	user, err := h.repository.GetByEmail(ctx, req.Email)
	if err != nil {
		log.Printf("failed get hashed user password: %v", err)

		return err
	}

	// Compare hashed password against plaintext password request
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
		log.Printf("failed to compare and hash password: %v", err)

		return err
	}

	token, err := h.tokenService.Encode(UnmarshalUser(user))
	if err != nil {
		log.Printf("failed to encode user: %v", err)

		return err
	}

	res.Token = token
	return nil
}

func (h *handler) Create(ctx context.Context, req *pb.User, res *pb.Response) error {
	hashedPass, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return err
	}

	req.Password = string(hashedPass)
	if err := h.repository.Create(ctx, MarshalUser(req)); err != nil {
		return err
	}

	// Strip the password back out, so's we're not returning it
	req.Password = ""
	res.User = req

	// Publish user created event
	if err := publishEvent(h.PubSub, req); err != nil {
		return err
	}

	return nil
}

func (h *handler) ValidateToken(ctx context.Context, req *pb.Token, res *pb.Token) error {
	claims, err := h.tokenService.Decode(req.Token)
	if err != nil {
		log.Printf("failed to decode token: %v", err)

		return err
	}

	if claims.User.Id == "" {
		return errors.New("invalid user")
	}

	res.Valid = true
	return nil
}

func publishEvent(publisher broker.Broker, user *pb.User) error {
	// Marshal to JSON string
	body, err := protojson.Marshal(user)
	if err != nil {
		return err
	}

	log.Printf("[pub] publishing user event: %s", string(body))

	// Create a broker message
	msg := &broker.Message{
		Header: map[string]string{
			"id": user.Id,
		},
		Body: body,
	}

	// Publish message to broker
	if err := publisher.Publish(topic, msg); err != nil {
		log.Printf("[pub] failed: %v", err)
	}

	return nil
}

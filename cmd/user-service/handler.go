package main

import (
	"context"
	"log"

	pb "github.com/murraystewart96/shippy/proto/user"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type authable interface {
	Decode(token string) (*CustomClaims, error)
	Encode(user *pb.User) (string, error)
}

type handler struct {
	pb.UnimplementedUserServiceServer
	repository   Repository
	tokenService authable
}

func (h *handler) Get(ctx context.Context, req *pb.User) (*pb.Response, error) {
	result, err := h.repository.Get(ctx, req.Id)
	if err != nil {
		return nil, status.Error(codes.NotFound, "user not found")
	}
	return &pb.Response{User: UnmarshalUser(result)}, nil
}

func (h *handler) GetAll(ctx context.Context, req *pb.Request) (*pb.Response, error) {
	results, err := h.repository.GetAll(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to get users")
	}
	return &pb.Response{Users: UnmarshalUserCollection(results)}, nil
}

func (h *handler) Auth(ctx context.Context, req *pb.User) (*pb.Token, error) {
	user, err := h.repository.GetByEmail(ctx, req.Email)
	if err != nil {
		log.Printf("auth: user lookup failed: %v", err)
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
		return nil, status.Error(codes.Unauthenticated, "invalid credentials")
	}

	token, err := h.tokenService.Encode(UnmarshalUser(user))
	if err != nil {
		log.Printf("auth: failed to encode token: %v", err)
		return nil, status.Error(codes.Internal, "failed to generate token")
	}

	return &pb.Token{Token: token}, nil
}

func (h *handler) Create(ctx context.Context, req *pb.User) (*pb.Response, error) {
	hashedPass, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to hash password")
	}

	req.Password = string(hashedPass)
	u := MarshalUser(req)
	if err := h.repository.Create(ctx, u); err != nil {
		log.Printf("create user: %v", err)
		return nil, status.Error(codes.Internal, "failed to create user")
	}

	created := UnmarshalUser(u)
	created.Password = ""
	return &pb.Response{User: created}, nil
}

func (h *handler) ValidateToken(ctx context.Context, req *pb.Token) (*pb.Token, error) {
	claims, err := h.tokenService.Decode(req.Token)
	if err != nil {
		log.Printf("validate token: %v", err)
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}

	if claims.User.Id == "" {
		return nil, status.Error(codes.Unauthenticated, "invalid token")
	}

	return &pb.Token{Valid: true}, nil
}

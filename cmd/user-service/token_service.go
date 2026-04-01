package main

import (
	"time"

	"github.com/dgrijalva/jwt-go"
	pb "github.com/murraystewart96/shippy/proto/user"
)

type CustomClaims struct {
	User *pb.User
	jwt.StandardClaims
}

type TokenService struct {
	repo      Repository
	jwtSecret string
}

func (ts TokenService) Decode(token string) (*CustomClaims, error) {
	tokenType, err := jwt.ParseWithClaims(token, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte(ts.jwtSecret), nil
	})
	if err != nil {
		return nil, err
	}

	if claims, ok := tokenType.Claims.(*CustomClaims); ok && tokenType.Valid {
		return claims, nil
	} else {
		return nil, err
	}
}

func (ts *TokenService) Encode(user *pb.User) (string, error) {
	// Create the claims
	claims := CustomClaims{
		user,
		jwt.StandardClaims{
			ExpiresAt: time.Now().Add(72 * time.Hour).Unix(),
			Issuer:    "shipping.UserService",
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	return token.SignedString([]byte(ts.jwtSecret))
}

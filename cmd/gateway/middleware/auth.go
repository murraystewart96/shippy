package middleware

import (
	"log"

	"github.com/dgrijalva/jwt-go"
	"github.com/gin-gonic/gin"
	userpb "github.com/murraystewart96/shippy/proto/user"
)

type CustomClaims struct {
	User *userpb.User
	jwt.StandardClaims
}

func Auth(jwtSecret string) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		token := ctx.GetHeader("x-token")
		if token == "" {
			ctx.AbortWithStatusJSON(401, gin.H{"error": "x-token header required"})
			return
		}

		claims, err := decodeJWT(token, jwtSecret)
		if err != nil {
			log.Printf("failed to validate token: %v", err)

			ctx.AbortWithStatusJSON(401, gin.H{"error": "invalid token"})
			return
		}

		if claims.User == nil || claims.User.Id == "" {
			log.Printf("failed to validate token: %v", err)

			ctx.AbortWithStatusJSON(401, gin.H{"error": "invalid token"})
			return
		}
	}
}

func decodeJWT(token, secret string) (*CustomClaims, error) {
	tokenType, err := jwt.ParseWithClaims(token, &CustomClaims{}, func(token *jwt.Token) (interface{}, error) {
		return []byte(secret), nil
	})
	if err != nil {
		return nil, err
	}

	if claims, ok := tokenType.Claims.(*CustomClaims); ok && tokenType.Valid {
		return claims, nil
	}

	return nil, err
}

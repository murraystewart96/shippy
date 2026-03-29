package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	userpb "github.com/murraystewart96/shippy/proto/user"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// POST /auth issues a JWT token for a valid user
func (h *handler) Auth(ctx *gin.Context) {
	var user struct {
		Email    string `json:"email" binding:"required,email"`
		Password string `json:"password" binding:"required"`
	}

	if err := ctx.ShouldBindJSON(&user); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	authResponse, err := h.userClient.Auth(ctx, &userpb.User{
		Email:    user.Email,
		Password: user.Password,
	})

	if err != nil {
		st, _ := status.FromError(err)
		if st.Code() == codes.Unauthenticated {
			ctx.JSON(http.StatusUnauthorized, gin.H{"error": "invalid credentials"})
			return
		}

		ctx.Status(http.StatusInternalServerError)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"token": authResponse.Token})
}

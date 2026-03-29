package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	userpb "github.com/murraystewart96/shippy/proto/user"
)

// POST /user issues a JWT token for a valid user
func (h *handler) CreateUser(ctx *gin.Context) {
	var user struct {
		Name     string `json:"name" binding:"required"`
		Email    string `json:"email" binding:"required,email"`
		Company  string `json:"company" binding:"required"`
		Password string `json:"password" binding:"required"`
	}

	if err := ctx.ShouldBindJSON(&user); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
		return
	}

	response, err := h.userClient.Create(ctx, &userpb.User{
		Name:     user.Name,
		Email:    user.Email,
		Company:  user.Company,
		Password: user.Password,
	})

	if err != nil {
		ctx.Status(http.StatusInternalServerError)
		return
	}

	ctx.JSON(http.StatusOK, gin.H{"id": response.User.Id})
}

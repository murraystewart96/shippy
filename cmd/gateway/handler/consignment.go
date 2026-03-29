package handler

import (
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	consignmentpb "github.com/murraystewart96/shippy/proto/consignment"
)

type container struct {
	CustomerId string `json:"customer_id" binding:"required"`
	Origin     string `json:"origin" binding:"required"`
	UserId     string `json:"user_id" binding:"required"`
}

type createConsignmentRequest struct {
	Description string      `json:"description" binding:"required"`
	Weight      int32       `json:"weight" binding:"required"`
	Containers  []container `json:"containers" binding:"required,min=1"`
}

// POST /v1/consignments
func (h *handler) CreateConsignment(ctx *gin.Context) {
	var req createConsignmentRequest
	if err := ctx.ShouldBindJSON(&req); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	containers := make([]*consignmentpb.Container, len(req.Containers))
	for i, c := range req.Containers {
		containers[i] = &consignmentpb.Container{
			CustomerId: c.CustomerId,
			Origin:     c.Origin,
			UserId:     c.UserId,
		}
	}

	response, err := h.consignmentClient.CreateConsignment(ctx, &consignmentpb.Consignment{
		Description: req.Description,
		Weight:      req.Weight,
		Containers:  containers,
	})
	if err != nil {
		log.Printf("failed to create consignment: %v", err)

		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create consignment"})
		return
	}

	ctx.JSON(http.StatusCreated, gin.H{"id": response.Consignment.Id})
}

// GET /v1/consignments
func (h *handler) GetConsignments(ctx *gin.Context) {
	response, err := h.consignmentClient.GetConsignments(ctx, &consignmentpb.GetRequest{})
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get consignments"})
		return
	}

	ctx.JSON(http.StatusOK, response.Consignments)
}

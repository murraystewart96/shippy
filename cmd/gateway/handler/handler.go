package handler

import (
	consignmentpb "github.com/murraystewart96/shippy/proto/consignment"
	userpb "github.com/murraystewart96/shippy/proto/user"
)

type handler struct {
	userClient        userpb.UserServiceClient
	consignmentClient consignmentpb.ConsignmentServiceClient
}

func New(uCli userpb.UserServiceClient, cCli consignmentpb.ConsignmentServiceClient) *handler {
	return &handler{
		userClient:        uCli,
		consignmentClient: cCli,
	}
}

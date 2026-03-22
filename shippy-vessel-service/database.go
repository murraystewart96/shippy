package main

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func CreateMongoClient(ctx context.Context, uri string, retry int32) (*mongo.Client, error) {
	conn, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx, nil); err != nil {
		if retry >= 3 {
			return nil, err
		}
		retry++
		time.Sleep(time.Second * 2)
		return CreateMongoClient(ctx, uri, retry)
	}

	return conn, err
}

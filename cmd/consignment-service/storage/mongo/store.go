package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
)

type Store struct {
	client *mongo.Client
}

func NewStore(client *mongo.Client) *Store {
	return &Store{client: client}
}

func (s *Store) WithTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	session, err := s.client.StartSession()
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(sessCtx mongo.SessionContext) (interface{}, error) {
		return nil, fn(sessCtx)
	})
	return err
}

package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/murraystewart96/shippy/reservation-service/config"
)

type DB struct {
	pool *pgxpool.Pool
}

func NewDB(cfg *config.DB) (*DB, error) {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)

	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	// Optional: configure pool settings
	poolConfig.MaxConns = 10
	poolConfig.MinConns = 2

	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test the connection
	err = pool.Ping(context.Background())
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{
		pool: pool,
	}, nil
}

func (db *DB) Close() {
	db.pool.Close()
}

func (db *DB) GetConn() *pgxpool.Pool {
	return db.pool
}

func (db *DB) Exec(query string) error {
	_, err := db.pool.Exec(context.Background(), query)
	return err
}

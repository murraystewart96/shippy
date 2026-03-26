package main

import (
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

func CreatePostgresClient(host, user, password, dbname string, retry int) (*sqlx.DB, error) {
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable", host, user, password, dbname)
	db, err := sqlx.Connect("postgres", dsn)
	if err != nil {
		if retry >= 3 {
			return nil, err
		}
		retry++
		time.Sleep(time.Second * 2)
		return CreatePostgresClient(host, user, password, dbname, retry)
	}
	return db, nil
}
package main

import (
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"log"
)

func main() {
	m, err := migrate.New(
		"file://migrations",
		"postgres://localhost:5432/fcfs?sslmode=disable",
	)
	if err != nil {
		log.Fatalf("unable to initialize migrations: %v", err)
	}
	err = m.Up()
	if err != nil {
		log.Fatalf("unable to apply migrations: %v", err)
	}
}

package postgres

import (
	"context"
	"embed"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/rs/zerolog/log"
)

//go:embed sql/migrations/*.sql
var embedMigrations embed.FS

func NewPostgresPool(connString string) *pgxpool.Pool {
	pool, err := pgxpool.New(
		context.Background(), connString,
	)
	if err != nil {
		log.Fatal().Msg("Unable to connect to database. Ensure DATABASE_URL is set correctly")
	}

	log.Debug().Msg("Running migrations")
	goose.SetBaseFS(embedMigrations)
	goose.SetLogger(goose.NopLogger())

	_ = goose.SetDialect("postgres")

	db := stdlib.OpenDBFromPool(pool)

	err = goose.Up(db, "sql/migrations")
	if err != nil {
		log.Fatal().Msgf("Unable to apply migrations: %v", err)
	}

	err = db.Close()
	if err != nil {
		log.Fatal().Msgf("Unable to close database connection: %v", err)
	}

	return pool
}

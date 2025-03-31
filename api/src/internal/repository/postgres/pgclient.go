// Package postgres defines a client for a PostgreSQL database that conforms to the
// DatabaseRepository interface in models.go. It uses the sqlc package to generate
// type-safe Go code from pure SQL queries.
package postgres

//go:generate sqlc generate

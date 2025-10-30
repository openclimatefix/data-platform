// Package postgres defines PostgreSQL implementations of the DataPlatform GRPC services backed by a PostgreSQL database.
// TODO: More comprehensive documentation
package postgres

import "embed"

//go:embed sql/migrations/*.sql
var Migrations embed.FS

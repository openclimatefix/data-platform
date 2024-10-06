# Datamodel

Migrations and queries that define schema of the database and associated SDKs.

## Running migrations

This project uses [golang migrate](https://github.com/golang-migrate/migrate) to manage database migrations.
Migrations are stored in the `migrations` directory; each with an 'up' and 'down' script.
Every migration is reversible allowing clean rollbacks.

To migrate a database, install the `migrate` CLI and use it as shown:

```bash
$ go install -tags 'postgres sqlite' github.com/golang-migrate/migrate/v4/cmd/migrate@latest
$ migrate -path migrations -database "postgres://user:password@localhost:5432/dbname?sslmode=disable" up
```

replacing the database connection string with the appropriate database instance.


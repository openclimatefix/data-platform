package main

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/jackc/pgx/v5"

	"github.com/devsjc/fcfs/datamodel/gen/datamodel"
)

// setupPostgres starts a postgres container to run tests against.
// It returns the connection string to the postgres container and a teardown function.
func setupPostgres(t *testing.T, ctx context.Context) (*pgx.Conn, func(*testing.T)) {
	t.Logf("Starting postgres container")
	postgresContainer, err := postgres.Run(ctx,
		"docker.io/postgres:16-alpine",
		postgres.WithDatabase("fcfs"),
		postgres.WithUsername("postgres"),
		postgres.WithPassword("postgres"),
		postgres.WithInitScripts("sql/migrations/*.sql"),
		postgres.BasicWaitStrategies(),
	)
	require.NoErrorf(t, err, "failed to start postgres container: %s", err)
	conn, err := pgx.Connect(ctx, postgresContainer.MustConnectionString(ctx))
	require.NoErrorf(t, err, "failed to connect to postgres container: %s", err)

	return conn, func(t *testing.T) {
		t.Log("Closing postgres connection")
		err = conn.Close(ctx)
		require.NoErrorf(t, err, "failed to close postgres connection: %s", err)
		t.Log("Tearing down postgres container")
		err := postgresContainer.Terminate(ctx)
		require.NoErrorf(t, err, "failed to terminate postgres container: %s", err)
	}
}

func TestQueries(t *testing.T) {
	ctx := context.Background()
	conn, teardown := setupPostgres(t, ctx)
	require.Contains(t, "postgres://postgres:postgres@localhost", conn)
	defer teardown(t)

	querier := datamodel.New(conn)

	// List all locations
	results, err := querier.ListSites(ctx)
	require.Equal(t, 0, len(results))

	// Add a site location
	result, err := querier.CreateLocationSite(ctx, datamodel.CreateLocationSiteParams{
		Name: "Site 1",
		Latitude: 66.6,
		Longitude: 77.7,
		Capacity: 100,
		ClientName: "acme-solar",
		ClientSiteID: "test-site-1",
		EnergySource: 1,
	})
	require.NoError(t, err)
	require.Equal(t, result, 1)	

}

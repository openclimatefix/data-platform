package test

import (
	"context"
	"testing"
	"time"

	"github.com/pressly/goose"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/jackc/pgx/v5"

	"github.com/devsjc/fcfs/datamodel/sdk/locations"
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
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(45*time.Second),
		),
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
	defer teardown(t)

	locationQuerier, err := locations.New(conn)

	// List all locations
	locations, err := queries.GetLocationByID(ctx, conn, 1)
	require.NoError(t, err)
	require.Equal(t, 0, locations.ID)

	// Add a site location
	result, err := queries.CreateLocationSite(ctx, conn, sdk.CreateLocationSiteParams{
		Name: "Site 1",
		Latitude: 66.6,
		Longitude: 77.7,
		CapacityKw: 100,
		ClientName: "acme-solar",
		ClientSiteID: "test-site-1",
		EnergySource: 1,
	})
	require.NoError(t, err)
	require.Equal(t, result.LocationID, 1)
	require.Equal(t, result.SiteID, 1)
		

	require.Contains(t, "postgres://postgres:postgres@localhost", conn)
}

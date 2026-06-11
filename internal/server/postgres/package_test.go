package postgres

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	protovalidate_ix "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
	ix "github.com/openclimatefix/data-platform/internal/interceptors"
)

var (
	// Global gRPC clients for use in tests.
	dc           pb.DataPlatformDataServiceClient
	pgConnString string
)

// TestMain defines the main entry point for testing.
func TestMain(m *testing.M) {
	code, err := setupTestMain(context.Background(), m)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to set up testing environment")
	}

	// Defer statements do not run past here due to os.Exit terminating the process.
	os.Exit(code)
}

// setupTestMain sets up the test dependencies and eventually executes the tests
// It is defined as a separate function to enable the setup stages to clearly defer their teardown
// steps (see https://google.github.io/styleguide/go/best-practices.html)
func setupTestMain(ctx context.Context, m *testing.M) (code int, err error) {
	// --- Create a Postgres container to use as a backend
	g := logConsumer{}
	req := testcontainers.ContainerRequest{
		FromDockerfile: testcontainers.FromDockerfile{
			Context:    filepath.Join(".", "infra"),
			Dockerfile: "Containerfile",
			KeepImage:  true,
		},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_DB":       "postgres",
		},
		Cmd: []string{
			"postgres",
			"-c",
			"fsync=off",
			"-c",
			"shared_preload_libraries=pg_cron",
		},
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor: wait.ForAll(
			wait.ForLog(
				"database system is ready to accept connections",
			).WithOccurrence(2),
			wait.ForListeningPort("5432/tcp"),
		),
		LogConsumerCfg: &testcontainers.LogConsumerConfig{
			Opts: []testcontainers.LogProductionOption{
				testcontainers.WithLogProductionTimeout(10 * time.Second),
			},
			Consumers: []testcontainers.LogConsumer{&g},
		},
		Name: fmt.Sprintf("dp_testdb_%d", time.Now().Unix()),
	}

	container, err := testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: req,
			Started:          true,
			Reuse:            true,
		},
	)
	if err != nil {
		return 0, err
	}
	defer container.Terminate(ctx)

	containerPort, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return 0, err
	}

	host, err := container.Host(ctx)
	if err != nil {
		return 0, err
	}

	pgConnString = fmt.Sprintf(
		"postgres://postgres:postgres@%s/postgres",
		net.JoinHostPort(host, containerPort.Port()),
	)

	// --- Set up gRPC clients for the DataPlatform services
	txInterceptor := ix.NewTransactionInterceptor(pgConnString, Migrations)
	logInterceptor := ix.NewLoggingInterceptor()
	validator, _ := protovalidate.New()
	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpc.UnaryServerInterceptor(protovalidate_ix.UnaryServerInterceptor(validator)),
			grpc.UnaryServerInterceptor(logInterceptor.UnaryServerInterceptor),
			grpc.UnaryServerInterceptor(txInterceptor.UnaryServerInterceptor),
		),
		grpc.ChainStreamInterceptor(
			grpc.StreamServerInterceptor(protovalidate_ix.StreamServerInterceptor(validator)),
			grpc.StreamServerInterceptor(logInterceptor.StreamServerInterceptor),
			grpc.StreamServerInterceptor(txInterceptor.StreamServerInterceptor),
		),
	)

	lis := bufconn.Listen(1024 * 1024)
	defer lis.Close()

	dataServerImpl := NewDataPlatformDataServiceServerImpl()

	pb.RegisterDataPlatformDataServiceServer(s, dataServerImpl)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Error().Err(err).Msg("Server exited with error")
		}
	}()

	defer s.GracefulStop()

	// Create client using same in-memory listener
	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	cc, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return 0, err
	}
	defer cc.Close()

	dc = pb.NewDataPlatformDataServiceClient(cc)

	// m.Run() executes the regular, user-defined test functions.
	// Any defer statements that have been made will be run after m.Run()
	// completes.
	return m.Run(), nil
}

// logConsumer consumes TestContainers logs via zerolog.
type logConsumer struct{}

// Accept implements the LogConsumer interface, only writing NOTICE logs.
func (lc *logConsumer) Accept(l testcontainers.Log) {
	if strings.Contains(string(l.Content), "DETAIL:") {
		log.Info().Msgf("postgres: %s", strings.Split(string(l.Content), "DETAIL: ")[1])
	}
}

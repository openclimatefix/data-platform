package main

import (
	"net"
	"os"
	"slices"
	"strings"

	"buf.build/go/protovalidate"
	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // GRPC will automatically negotiate and use gzip if the client supports it.
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	dbdy "github.com/openclimatefix/data-platform/internal/database/dummy"
	dbpg "github.com/openclimatefix/data-platform/internal/database/postgres"
	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

func main() {
	// Set logging level based on environment
	logLevel, err := zerolog.ParseLevel(os.Getenv("LOGLEVEL"))
	if err != nil {
		logLevel = zerolog.InfoLevel
	}

	zerolog.SetGlobalLevel(logLevel)

	// Open a listener on port 50051
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal().Err(err).Msg("net.Listen({tcp: 500051})")
	}

	// Create a validator to use with protovalidate interceptor
	validator, err := protovalidate.New()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create validator")
	}

	// Choose the server implementation based on the environment
	databaseUrl := os.Getenv("DATABASE_URL")

	var (
		dataServerImpl  pb.DataPlatformDataServiceServer
		adminServerImpl pb.DataPlatformAdministrationServiceServer
		s               *grpc.Server
	)

	if slices.Contains([]string{"", "dummy", "fake"}, strings.ToLower(databaseUrl)) {
		log.Warn().Msg("Running in test mode with fake data. Not for production use")

		dataServerImpl = dbdy.NewDataPlatformDataServerImpl()
		adminServerImpl = dbdy.NewDataPlatformAdministrationServiceServerImpl()

		// For a dummy-backed server, just validate requests
		s = grpc.NewServer(
			grpc.ChainUnaryInterceptor(
				grpc.UnaryServerInterceptor(middleware.UnaryServerInterceptor(validator)),
			),
		)
	} else if strings.HasPrefix(databaseUrl, "postgres") && strings.Contains(databaseUrl, "://") {
		log.Info().Str("type", "postgresql").Msg("Connecting to database backend")

		txInjector := dbpg.NewTransactionInjector(databaseUrl)
		dataServerImpl = dbpg.NewDataPlatformDataServiceServerImpl()
		adminServerImpl = dbpg.NewDataPlatformAdministrationServiceServerImpl()

		// For a postgres-backed server, validate requests and manage database transactions
		s = grpc.NewServer(
			grpc.ChainUnaryInterceptor(
				grpc.UnaryServerInterceptor(middleware.UnaryServerInterceptor(validator)),
				grpc.UnaryServerInterceptor(txInjector.UnaryServerInterceptor),
			),
		)
	} else {
		log.Fatal().Str("url", databaseUrl).Msg("Unsupported DATABASE_URL format")
	}

	// Create the GRPC server
	// * Add an interceptor for request validation
	log.Info().Int("port", 50051).Msg("Starting GRPC server")

	pb.RegisterDataPlatformDataServiceServer(s, dataServerImpl)
	pb.RegisterDataPlatformAdministrationServiceServer(s, adminServerImpl)
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	reflection.Register(s)

	log.Info().Msg("Listening on :50051")
	_ = s.Serve(lis) // If this errors, we want it to panic! It's fundamental
}

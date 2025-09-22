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

	dbdy "github.com/devsjc/fcfs/internal/database/dummy"
	dbpg "github.com/devsjc/fcfs/internal/database/postgres"
	pb "github.com/devsjc/fcfs/internal/gen/ocf/dp"
)

func main() {
	// Set logging level based on environment
	logLevel, err := zerolog.ParseLevel(os.Getenv("LOGLEVEL"))
	if err != nil {
		logLevel = zerolog.InfoLevel
	}

	zerolog.SetGlobalLevel(logLevel)

	// Choose the server implementation based on the environment
	databaseUrl := os.Getenv("DATABASE_URL")

	var dpServerImpl pb.DataPlatformServiceServer

	if slices.Contains([]string{"", "dummy", "fake"}, strings.ToLower(databaseUrl)) {
		log.Warn().Msg("Running in test mode with fake data. Not for production use")

		dpServerImpl = dbdy.NewDummyDataPlatformServerImpl()
	} else if strings.HasPrefix(databaseUrl, "postgres") && strings.Contains(databaseUrl, "://") {
		log.Info().Str("type", "postgresql").Msg("Connecting to database backend")

		dpServerImpl = dbpg.NewPostgresDataPlatformServerImpl(databaseUrl)
	} else {
		log.Fatal().Str("url", databaseUrl).Msg("Unsupported DATABASE_URL format")
	}

	// Create the GRPC server
	// * Add an interceptor for request validation
	log.Info().Int("port", 50051).Msg("Starting GRPC server")

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to listen")
	}

	validator, err := protovalidate.New()
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create validator")
	}

	s := grpc.NewServer(
		grpc.UnaryInterceptor(middleware.UnaryServerInterceptor(validator)),
	)
	pb.RegisterDataPlatformServiceServer(s, dpServerImpl)
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	reflection.Register(s)
	log.Info().Msg("Listening on :50051")

	_ = s.Serve(lis) // If this errors, we want it to panic! It's fundamental
}

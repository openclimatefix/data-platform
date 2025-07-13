package main

import (
	"net"
	"os"
	"slices"
	"strings"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	// Installing the gzip encoding registers it as an available compressor.
	// gRPC will automatically negotiate and use gzip if the client supports it.
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/reflection"

	"buf.build/go/protovalidate"
	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	dbpg "github.com/devsjc/fcfs/dp/internal/database/postgres"
	pb "github.com/devsjc/fcfs/dp/internal/gen/ocf/dp"
)

func main() {
	// Set logging level based on environment
	logLevel, err := zerolog.ParseLevel(os.Getenv("LOGLEVEL"))
	if err != nil {
		logLevel = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(logLevel)

	databaseUrl := os.Getenv("DATABASE_URL")
	var dpServerImpl pb.DataPlatformServiceServer
	if slices.Contains([]string{"", "dummy", "fake"}, strings.ToLower(databaseUrl)) {
		log.Info().Msg("Running in test mode with fake data")
		log.Fatal().Msg("Not yet implemented!")
	} else if strings.HasPrefix(databaseUrl, "postgres") && strings.Contains(databaseUrl, "://") {
		log.Debug().Str("type", "postgresql").Msg("Connecting to database backend")
		dpServerImpl = dbpg.NewPostgresDataPlatformServerImpl(databaseUrl)
	} else {
		log.Fatal().Str("url", databaseUrl).Msg("Unsupported DATABASE_URL format")
	}

	log.Info().Int("port", 50051).Msg("Starting GRPC server")
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to listen")
	}

	// Create the GRPC server
	// * Add an interceptor for request validation
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
	s.Serve(lis)
}

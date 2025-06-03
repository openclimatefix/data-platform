package main

import (
	"net"
	"os"
	"strconv"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/devsjc/fcfs/api/src/internal/models/fcfsapi"
	rpgx "github.com/devsjc/fcfs/api/src/internal/repository/postgres"
)

func main() {
	// Set logging level based on environment
	logLevel, err := strconv.Atoi(os.Getenv("LOGLEVEL"))
	if err != nil {
		logLevel = int(zerolog.InfoLevel)
	}
	zerolog.SetGlobalLevel(zerolog.Level(logLevel))

	log.Debug().Str("type", os.Getenv("DATABASE_TYPE")).Msg("Connecting to backend")
	connString := os.Getenv("DATABASE_URL")
	apiServer := rpgx.NewQuartzAPIPostgresServer(connString)
	log.Info().Msg("Server connected to database instance")

	log.Info().Int("port", 50051).Msg("Starting GRPC server")
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to listen")
	}
	s := grpc.NewServer()
	fcfsapi.RegisterQuartzAPIServer(s, apiServer)
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	reflection.Register(s)
	log.Info().Msg("Listening on :50051")
	s.Serve(lis)
};

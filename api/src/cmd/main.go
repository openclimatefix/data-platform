package main

import (
	"net"
	"os"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/devsjc/fcfs/api/src/internal/models/fcfsapi"
	rpgx "github.com/devsjc/fcfs/api/src/internal/repository/postgres"
)

func main() {
	log.Debug().Str("type", os.Getenv("DATABASE_TYPE")).Msg("Connecting to backend")
	connString := os.Getenv("DATABASE_URL")
	apiServer := rpgx.NewQuartzAPIPostgresServer(connString)
	log.Info().Msg("ApiServer created")

	log.Info().Int("port", 50051).Msg("Starting GRPC server")
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to listen")
	}
	s := grpc.NewServer()
	fcfsapi.RegisterQuartzAPIServer(s, apiServer)
	reflection.Register(s)
	log.Info().Msg("Listening on :50051")
	s.Serve(lis)
};

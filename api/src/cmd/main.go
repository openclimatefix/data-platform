package main

import (
	"net"

	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	service "github.com/openclimatefix/api/src/internal/service"
	repository "github.com/openclimatefix/api/src/internal/repository"
	pb "github.com/openclimatefix/api/src/proto"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	log.Info().Msg("Listening on :50051")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to listen")
	}
	s := grpc.NewServer()
	apiServer := &service.APIServer{DBS: &repository.DummyClient{}}
	pb.RegisterQuartzAPIServer(s, apiServer)
	reflection.Register(s)
	s.Serve(lis)
}

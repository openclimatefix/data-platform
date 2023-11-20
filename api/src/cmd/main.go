package main

import (
	"context"
	"net"

	internal "github.com/openclimatefix/api/src/internal"
	respository "github.com/openclimatefix/api/src/internal/repository"
	pb "github.com/openclimatefix/api/src/proto"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type apiServer struct {
	pb.UnimplementedQuartzAPIServer
	dbs internal.DatabaseService
}

func (s *apiServer) GetPredictedTimeseries(req *pb.GetPredictedTimeseriesRequest, stream pb.QuartzAPI_GetPredictedTimeseriesServer) (err error) {
	log.Info().Msg("GetPredictedTimeseries called")

	for _, locID := range req.GetLocationIDs() {
		// Fetch yields using database service
		fetchedYields, err := s.dbs.GetPredictedYieldsForLocation(locID)

		// Map fetched yields to protobuf yields
		returnYields := make([]*pb.PredictedYield, len(fetchedYields))
		for i, yield := range fetchedYields {
			returnYields[i] = &pb.PredictedYield{
				TimestampUnix: int64(yield.TimeUnix),
				YieldKw:       yield.YieldKW,
				Error: &pb.PredictedYieldError{
					UpperKw: yield.ErrHigh,
					LowerKw: yield.ErrLow,
				},
			}
		}

		// Stream yields for location to client
		err = stream.Send(&pb.GetPredictedTimeseriesResponse{
			LocationID: locID,
			Yields:     returnYields,
		})
		if err != nil {
			log.Warn().Err(err).Msg("Error sending response")
		}
	}

	return nil
}

func (s *apiServer) GetActualTimeseries(req *pb.GetActualTimeseriesRequest, stream pb.QuartzAPI_GetActualTimeseriesServer) (err error) {
	log.Info().Msg("GetActualTimeseries called")

	for _, locID := range req.GetLocationIDs() {
		// Fetch yields using database service
		fetchedYields, err := s.dbs.GetActualYieldsForLocation(locID)
		if err != nil {
			log.Warn().Err(err).Str("locationID", locID).Msg("Error fetching data")
		}

		// Map fetched yields to protobuf yields
		returnYields := make([]*pb.ActualYield, len(fetchedYields))
		for i, yield := range fetchedYields {
			returnYields[i] = &pb.ActualYield{
				TimestampUnix: int64(yield.TimeUnix),
				YieldKw:       yield.YieldKW,
			}
		}

		// Stream yields for location to client
		err = stream.Send(&pb.GetActualTimeseriesResponse{
			LocationID: locID,
			Yields:     returnYields,
		})
		if err != nil {
			log.Warn().Err(err).Msg("Error sending response")
		}
	}

	return nil
}

func (s *apiServer) GetPredictedCrossSection(ctx context.Context, req *pb.GetPredictedCrossSectionRequest) (*pb.GetPredictedCrossSectionResponse, error) {
	log.Info().Msg("GetPredictedCrossSection called")

	// Fetch data using database service
	fetchedYields, err := s.dbs.GetPredictedYieldForLocations(req.GetLocationIDs(), int32(req.GetTimestampUnix()))
	if err != nil {
		log.Warn().Err(err).Msg("Error fetching data")
		return nil, err
	}

	// Map the fetched yield to protobuf yields
	returnYields := make([]*pb.PredictedYieldAtLocation, len(fetchedYields))
	for i, yield := range fetchedYields {
		returnYields[i] = &pb.PredictedYieldAtLocation{
			LocationID: yield.LocationID,
			YieldKw:    yield.YieldKW,
			Error: &pb.PredictedYieldError{
				UpperKw: yield.ErrHigh,
				LowerKw: yield.ErrLow,
			},
		}
	}

	return &pb.GetPredictedCrossSectionResponse{
		Yields:        returnYields,
		TimestampUnix: req.GetTimestampUnix(),
	}, nil
}

func (s *apiServer) GetActualCrossSection(ctx context.Context, req *pb.GetActualCrossSectionRequest) (*pb.GetActualCrossSectionResponse, error) {
	log.Info().Msg("GetActualCrossSection called")

	// Fetch data using database service
	fetchedYields, err := s.dbs.GetActualYieldForLocations(req.GetLocationIDs(), int32(req.GetTimestampUnix()))
	if err != nil {
		log.Warn().Err(err).Msg("Error fetching data")
		return nil, err
	}

	// Map the fetched yield to protobuf yields
	returnYields := make([]*pb.ActualYieldAtLocation, len(fetchedYields))
	for i, yield := range fetchedYields {
		returnYields[i] = &pb.ActualYieldAtLocation{
			LocationID: yield.LocationID,
			YieldKw:    yield.YieldKW,
		}
	}

	return &pb.GetActualCrossSectionResponse{
		Yields:        returnYields,
		TimestampUnix: req.GetTimestampUnix(),
	}, nil
}

func (s *apiServer) GetLocationMetadata(ctx context.Context, req *pb.GetLocationMetadataRequest) (*pb.GetLocationMetadataResponse, error) {
	log.Info().Msg("GetLocationMetadata called")
	return &pb.GetLocationMetadataResponse{}, nil
}

func main() {
	lis, err := net.Listen("tcp", ":50051")
	log.Info().Msg("Listening on :50051")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to listen")
	}
	s := grpc.NewServer()
	apiServer := &apiServer{dbs: &respository.DummyClient{}}
	pb.RegisterQuartzAPIServer(s, apiServer)
	reflection.Register(s)
	s.Serve(lis)
}

package service

import (
	"context"

	internal "github.com/openclimatefix/api/src/internal"
	pb "github.com/openclimatefix/api/src/proto"
	"github.com/rs/zerolog/log"
)

type APIServer struct {
	pb.UnimplementedQuartzAPIServer
	DBS internal.DatabaseService
}

func (s *APIServer) GetPredictedTimeseries(req *pb.GetPredictedTimeseriesRequest, stream pb.QuartzAPI_GetPredictedTimeseriesServer) (err error) {
	log.Info().Msg("GetPredictedTimeseries called")

	for _, locID := range req.GetLocationIDs() {
		// Fetch yields using database service
		fetchedYields, err := s.DBS.GetPredictedYieldsForLocation(locID)

		// Map fetched yields to protobuf yields
		returnYields := make([]*pb.PredictedYield, len(fetchedYields))
		for i, yield := range fetchedYields {
			returnYields[i] = &pb.PredictedYield{
				TimestampUnix: int64(yield.TimeUnix),
				YieldKw:       int32(yield.YieldKW),
				Uncertainty: &pb.PredictedYieldUncertainty{
					UpperKw: int32(yield.ErrHigh),
					LowerKw: int32(yield.ErrLow),
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


func (s *APIServer) GetActualTimeseries(req *pb.GetActualTimeseriesRequest, stream pb.QuartzAPI_GetActualTimeseriesServer) (err error) {
	log.Info().Msg("GetActualTimeseries called")

	for _, locID := range req.GetLocationIDs() {
		// Fetch yields using database service
		fetchedYields, err := s.DBS.GetActualYieldsForLocation(locID)
		if err != nil {
			log.Warn().Err(err).Str("locationID", locID).Msg("Error fetching data")
		}

		// Map fetched yields to protobuf yields
		returnYields := make([]*pb.ActualYield, len(fetchedYields))
		for i, yield := range fetchedYields {
			returnYields[i] = &pb.ActualYield{
				TimestampUnix: int64(yield.TimeUnix),
				YieldKw:       int32(yield.YieldKW),
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

func (s *APIServer) GetPredictedCrossSection(ctx context.Context, req *pb.GetPredictedCrossSectionRequest) (*pb.GetPredictedCrossSectionResponse, error) {
	log.Info().Msg("GetPredictedCrossSection called")

	// Fetch data using database service
	fetchedYields, err := s.DBS.GetPredictedYieldForLocations(req.GetLocationIDs(), req.GetTimestampUnix())
	if err != nil {
		log.Warn().Err(err).Msg("Error fetching data")
		return nil, err
	}

	// Map the fetched yield to protobuf yields
	returnYields := make([]*pb.PredictedYieldAtLocation, len(fetchedYields))
	for i, yield := range fetchedYields {
		returnYields[i] = &pb.PredictedYieldAtLocation{
			LocationID: yield.LocationID,
			YieldKw:    int32(yield.YieldKW),
			Uncertainty: &pb.PredictedYieldUncertainty{
				UpperKw: int32(yield.ErrHigh),
				LowerKw: int32(yield.ErrLow),
			},
		}
	}

	return &pb.GetPredictedCrossSectionResponse{
		Yields:        returnYields,
		TimestampUnix: req.GetTimestampUnix(),
	}, nil
}

func (s *APIServer) GetActualCrossSection(ctx context.Context, req *pb.GetActualCrossSectionRequest) (*pb.GetActualCrossSectionResponse, error) {
	log.Info().Msg("GetActualCrossSection called")

	// Fetch data using database service
	fetchedYields, err := s.DBS.GetActualYieldForLocations(req.GetLocationIDs(), req.GetTimestampUnix())
	if err != nil {
		log.Warn().Err(err).Msg("Error fetching data")
		return nil, err
	}

	// Map the fetched yield to protobuf yields
	returnYields := make([]*pb.ActualYieldAtLocation, len(fetchedYields))
	for i, yield := range fetchedYields {
		returnYields[i] = &pb.ActualYieldAtLocation{
			LocationID: yield.LocationID,
			YieldKw:    int32(yield.YieldKW),
		}
	}

	return &pb.GetActualCrossSectionResponse{
		Yields:        returnYields,
		TimestampUnix: req.GetTimestampUnix(),
	}, nil
}

func (s *APIServer) GetLocationMetadata(ctx context.Context, req *pb.GetLocationMetadataRequest) (*pb.GetLocationMetadataResponse, error) {
	log.Info().Msg("GetLocationMetadata called")
	return &pb.GetLocationMetadataResponse{}, nil
}

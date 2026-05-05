package postgres

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

func seed(tb testing.TB, pgConnString string, params seedDBParams) (output struct {
	NumPgvs       int
	LocationUuids []string
},
) {
	seedfiles, _ := filepath.Glob(filepath.Join(".", "testdata", "seed*.sql"))
	conn, err := pgx.Connect(tb.Context(), pgConnString)

	require.NoError(tb, err)

	defer func() {
		err = conn.Close(tb.Context())
		require.NoError(tb, err)
	}()

	for _, f := range seedfiles {
		sql, err := os.ReadFile(f)
		require.NoError(tb, err)

		_, err = conn.Exec(tb.Context(), string(sql))
		require.NoError(tb, err)

		var result struct {
			NumPgvs       int
			LocationUuids []pgtype.UUID
		}

		err = conn.QueryRow(
			tb.Context(),
			fmt.Sprintf(
				"SELECT seed_db("+
					"name_prefix=>'%s'::TEXT,"+
					"target_locations=>%d::INTEGER,"+
					"history_window_mins=>%d::INTEGER,"+
					"pivot_time=>'%s'::TIMESTAMP"+
					");",
				params.NamePrefix,
				params.NumLocations,
				params.HistoryWindowMins,
				params.PivotTime.UTC().Format(time.RFC3339),
			),
		).Scan(&result)
		require.NoError(tb, err)
		tb.Logf(
			"Seeded %d predicted generation values for %d locations",
			output.NumPgvs,
			len(output.LocationUuids),
		)

		stringUuids := make([]string, len(result.LocationUuids))
		for i, u := range result.LocationUuids {
			stringUuids[i] = u.String()
		}

		output.NumPgvs = result.NumPgvs
		output.LocationUuids = stringUuids
	}

	return output
}

type seedDBParams struct {
	NamePrefix        string
	NumLocations      int
	HistoryWindowMins int
	PivotTime         time.Time
}

func truncate(tb testing.TB, pgConnString string) {
	conn, err := pgx.Connect(tb.Context(), pgConnString)
	require.NoError(tb, err)

	defer func() {
		err = conn.Close(tb.Context())
		require.NoError(tb, err)
	}()

	_, err = conn.Exec(tb.Context(), `
	    TRUNCATE TABLE
		loc.geometries,
		pred.forecasters,
		obs.observers
	    CASCADE;
        `)

	require.NoError(tb, err)
}

var yields = func() []*pb.CreateForecastRequest_ForecastValue {
	var values []*pb.CreateForecastRequest_ForecastValue
	for i := range 48 * 2 {
		values = append(values, &pb.CreateForecastRequest_ForecastValue{
			P50Fraction: 0.2,
			OtherStatisticsFractions: map[string]float32{
				"p10": 0.1,
				"p90": 0.3,
			},
			HorizonMins: uint32(i * 30),
			Metadata: func() *structpb.Struct {
				s, _ := structpb.NewStruct(map[string]any{
					"example_key": fmt.Sprintf("example_value_%d", i),
				})
				return s
			}(),
		})
	}

	return values
}

// --- BENCHMARKS ---------------------------------------------------------------------------------

// BenchmarkPostgresClient runs benchmarks against the Postgres client.
// It does not test for the validity of the responses, as these are covered in the unit test cases.
// Instead it determines how long each RPC takes to complete against a database of a given size.
func BenchmarkPostgresClient(b *testing.B) {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	pivotTime := time.Now().Truncate(24 * time.Hour).Add(-7 * 24 * time.Hour)

	testcases := []seedDBParams{
		{
			NumLocations:      100,
			HistoryWindowMins: 60 * 48,
			PivotTime:         pivotTime,
			NamePrefix:        "small",
		},
	}

	for _, tc := range testcases {
		b.Run(tc.NamePrefix, func(b *testing.B) {
			truncate(b, pgConnString)
			output := seed(b, pgConnString, tc)

			b.Run("GetForecastAsTimeseries", func(b *testing.B) {
				for b.Loop() {
					resp, err := dc.GetForecastAsTimeseries(
						b.Context(),
						&pb.GetForecastAsTimeseriesRequest{
							LocationUuid: output.LocationUuids[0],
							EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
							Forecaster: &pb.Forecaster{
								ForecasterName:    tc.NamePrefix + "_forecaster_1",
								ForecasterVersion: "v1",
							},
							TimeWindow: &pb.TimeWindow{
								StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 48)),
								EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour * 36)),
							},
						},
					)
					require.NoError(b, err)
					// There is a forecast value every 30 minutes, and the window is 84 hours long
					// But the forecast made at the pivot time is the latest one, and that is only
					// 12 hours long
					require.GreaterOrEqual(
						b,
						len(resp.Values),
						(48+12)*60/30,
					)
				}
			})
			b.Run("GetForecastAtTimestamp", func(b *testing.B) {
				if len(output.LocationUuids) > 100 {
					output.LocationUuids = output.LocationUuids[0:100]
				}

				for b.Loop() {
					crossSectionResp, err := dc.GetForecastAtTimestamp(
						b.Context(),
						&pb.GetForecastAtTimestampRequest{
							EnergySource:  pb.EnergySource_ENERGY_SOURCE_SOLAR,
							LocationUuids: output.LocationUuids,
							Forecaster: &pb.Forecaster{
								ForecasterName:    tc.NamePrefix + "_forecaster_1",
								ForecasterVersion: "v1",
							},
							TimestampUtc: timestamppb.New(pivotTime),
						},
					)
					require.NoError(b, err)
					require.NotNil(b, crossSectionResp)
					require.Equal(b, len(output.LocationUuids), len(crossSectionResp.Values))
				}
			})
			b.Run("GetObservationsAsTimeseries", func(b *testing.B) {
				for b.Loop() {
					obsResp, err := dc.GetObservationsAsTimeseries(
						b.Context(),
						&pb.GetObservationsAsTimeseriesRequest{
							LocationUuid: output.LocationUuids[0],
							ObserverName: tc.NamePrefix + "_observer",
							EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
							TimeWindow: &pb.TimeWindow{
								StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 36)),
								EndTimestampUtc:   timestamppb.New(pivotTime),
							},
						},
					)
					require.NoError(b, err)
					require.GreaterOrEqual(b, len(obsResp.Values), 36*60/30)
				}
			})
			b.Run("CreateForecast", func(b *testing.B) {
				for b.Loop() {
					_, err := dc.CreateForecast(b.Context(), &pb.CreateForecastRequest{
						Forecaster: &pb.Forecaster{
							ForecasterName:    tc.NamePrefix + "_forecaster_1",
							ForecasterVersion: "v1",
						},
						LocationUuid: output.LocationUuids[0],
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						InitTimeUtc: timestamppb.New(
							pivotTime.Add(
								time.Duration(12+2) * time.Hour,
							),
						),
						Values: yields(),
					})
					require.NoError(b, err)
					_, err = dc.DeleteForecast(b.Context(), &pb.DeleteForecastRequest{
						Forecaster: &pb.Forecaster{
							ForecasterName:    tc.NamePrefix + "_forecaster_1",
							ForecasterVersion: "v1",
						},
						LocationUuid: output.LocationUuids[0],
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						InitTimeUtc: timestamppb.New(
							pivotTime.Add(time.Duration(12+2) * time.Hour),
						),
					})
					require.NoError(b, err)
				}
			})
			b.Run("StreamForecastData", func(b *testing.B) {
				for b.Loop() {
					stream, err := dc.StreamForecastData(
						b.Context(),
						&pb.StreamForecastDataRequest{
							LocationUuids: []string{
								output.LocationUuids[0],
								output.LocationUuids[1],
							},
							EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
							Forecasters: []*pb.Forecaster{{
								ForecasterName:    tc.NamePrefix + "_forecaster_1",
								ForecasterVersion: "v1",
							}},
							TimeWindow: &pb.StreamForecastDataRequest_TimeWindow{
								StartTimestampUtc: timestamppb.New(pivotTime.Add(-time.Hour * 48)),
								EndTimestampUtc:   timestamppb.New(pivotTime.Add(time.Hour * 36)),
							},
						},
					)
					require.NoError(b, err)

					var numValues int
					for {
						_, err := stream.Recv()
						if err != nil {
							break
						}

						numValues++
					}

					require.GreaterOrEqual(
						b,
						numValues,
						(48+12)*60/30,
					)
				}
			})
		})
	}
}

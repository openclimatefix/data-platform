package ui

import (
	"context"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

type seriesResult struct {
	Raw         string
	SeriesMap   map[int64]float32
	StatsMap    map[int64]map[string]float32
	UniqueT     map[int64]time.Time
	FractionSum float32
	Count       int
}

func fetchConcurrently[T any](ctx context.Context, items []T, fetchFn func(context.Context, T) (seriesResult, error)) []seriesResult {
	results := make([]seriesResult, len(items))
	g, gCtx := errgroup.WithContext(ctx)

	for i, item := range items {
		// 3.3 Drop obsolete loop captures
		g.Go(func() error {
			res, err := fetchFn(gCtx, item)
			if err != nil {
				log.Warn().Err(err).Msg("Failed to fetch series data")
			} else {
				results[i] = res
			}
			return nil
		})
	}
	_ = g.Wait()
	return results
}

func fetchForecasters(ctx context.Context, client pb.DataPlatformDataServiceClient, p *forecastQuery) []seriesResult {
	return fetchConcurrently(ctx, p.Forecasters, func(gCtx context.Context, f ForecasterInput) (seriesResult, error) {
		req := &pb.GetForecastAsTimeseriesRequest{
			LocationUuid: p.LocUUID,
			EnergySource: pb.EnergySource(p.EnergySource),
			HorizonMins:  uint32(f.HorizonMins),
			TimeWindow: &pb.TimeWindow{
				StartTimestampUtc: timestamppb.New(p.StartTs),
				EndTimestampUtc:   timestamppb.New(p.EndTs),
			},
			Forecaster: &pb.Forecaster{ForecasterName: f.Name, ForecasterVersion: f.Version},
		}

		resp, err := client.GetForecastAsTimeseries(gCtx, req)
		if err != nil {
			return seriesResult{}, fmt.Errorf("failed to get forecast for %s: %w", f.Raw, err)
		}

		res := seriesResult{
			Raw:       f.Raw,
			SeriesMap: make(map[int64]float32),
			StatsMap:  make(map[int64]map[string]float32),
			UniqueT:   make(map[int64]time.Time),
		}

		for _, v := range resp.GetValues() {
			t := v.GetTargetTimestampUtc().AsTime()
			unix := t.Unix()
			res.UniqueT[unix] = t
			res.SeriesMap[unix] = v.GetP50ValueFraction()

			if stats := v.GetOtherStatisticsFractions(); stats != nil {
				res.StatsMap[unix] = make(map[string]float32)
				for k, val := range stats {
					if strings.HasPrefix(k, "p") {
						res.StatsMap[unix][k] = val
					}
				}
			}

			res.FractionSum += v.GetP50ValueFraction()
			res.Count++
		}
		return res, nil
	})
}

func fetchObservers(ctx context.Context, client pb.DataPlatformDataServiceClient, p *forecastQuery) []seriesResult {
	return fetchConcurrently(ctx, p.Observers, func(gCtx context.Context, o ObserverInput) (seriesResult, error) {
		req := &pb.GetObservationsAsTimeseriesRequest{
			LocationUuid: p.LocUUID,
			ObserverName: o.Name,
			EnergySource: pb.EnergySource(p.EnergySource),
			TimeWindow: &pb.TimeWindow{
				StartTimestampUtc: timestamppb.New(p.StartTs),
				EndTimestampUtc:   timestamppb.New(p.EndTs),
			},
		}

		resp, err := client.GetObservationsAsTimeseries(gCtx, req)
		if err != nil {
			return seriesResult{}, fmt.Errorf("failed to get observations for %s: %w", o.Raw, err)
		}

		res := seriesResult{
			Raw:       o.Raw,
			SeriesMap: make(map[int64]float32),
			UniqueT:   make(map[int64]time.Time),
		}

		for _, v := range resp.GetValues() {
			t := v.GetTimestampUtc().AsTime()
			unix := t.Unix()
			res.UniqueT[unix] = t
			res.SeriesMap[unix] = v.GetValueFraction()
		}
		return res, nil
	})
}

func buildChartSeries(
	results []seriesResult,
	obsResults []seriesResult,
	forecasters []ForecasterInput,
	observers []ObserverInput,
	capacity float32,
) ([]SeriesData, []string, []int64, float32) {
	var allSeries []SeriesData
	uniqueTimes := make(map[int64]time.Time)
	forecasterResults := make(map[string]map[int64]float32)
	forecasterStats := make(map[string]map[int64]map[string]float32)

	var (
		totalFraction float32
		count         int
	)

	for _, res := range results {
		if res.SeriesMap == nil {
			continue
		}

		forecasterResults[res.Raw] = res.SeriesMap
		forecasterStats[res.Raw] = res.StatsMap

		for unix, t := range res.UniqueT {
			uniqueTimes[unix] = t
		}

		totalFraction += res.FractionSum
		count += res.Count
	}

	for _, res := range obsResults {
		if res.SeriesMap == nil {
			continue
		}

		forecasterResults[res.Raw] = res.SeriesMap
		for unix, t := range res.UniqueT {
			uniqueTimes[unix] = t
		}
	}

	var timeKeys []int64
	for k := range uniqueTimes {
		timeKeys = append(timeKeys, k)
	}

	slices.Sort(timeKeys)

	var labels []string
	for _, k := range timeKeys {
		labels = append(labels, uniqueTimes[k].Format("Jan 02 15:04"))
	}

	for _, f := range forecasters {
		sd := SeriesData{
			Name:    fmt.Sprintf("%s (v%s) @ %dm", f.Name, f.Version, f.HorizonMins),
			BandMap: make([]map[string]*float32, len(timeKeys)),
		}

		pPairs := make(map[string]struct{})
		for _, k := range timeKeys {
			if stats, ok := forecasterStats[f.Raw][k]; ok {
				for pKey := range stats {
					if len(pKey) > 1 {
						pVal, err := strconv.Atoi(pKey[1:])
						if err == nil && pVal < 50 {
							complement := fmt.Sprintf("p%d", 100-pVal)
							if _, hasComplement := stats[complement]; hasComplement {
								pPairs[fmt.Sprintf("%d", pVal)] = struct{}{}
							}
						}
					}
				}
			}
		}

		type PLevelPair struct {
			Level     int
			LowerName string
			UpperName string
		}

		var activePairs []PLevelPair
		for k := range pPairs {
			pVal, _ := strconv.Atoi(k)
			activePairs = append(activePairs, PLevelPair{
				Level:     100 - (pVal * 2),
				LowerName: fmt.Sprintf("p%d", pVal),
				UpperName: fmt.Sprintf("p%d", 100-pVal),
			})
		}

		slices.SortFunc(activePairs, func(a, b PLevelPair) int {
			return b.Level - a.Level
		})

		if len(activePairs) > 0 {
			sd.HasBands = true
			for _, pair := range activePairs {
				sd.Bands = append(sd.Bands, BandLayer{
					Level:     pair.Level,
					LowerName: pair.LowerName,
					UpperName: pair.UpperName,
					Lower:     make([]*float32, len(timeKeys)),
					Diff:      make([]*float32, len(timeKeys)),
				})
			}
		}

		for idx, k := range timeKeys {
			sd.BandMap[idx] = make(map[string]*float32)

			if val, ok := forecasterResults[f.Raw][k]; ok {
				v := float32(math.Round(float64(val*capacity)*100) / 100)
				sd.Data = append(sd.Data, &v)
			} else {
				sd.Data = append(sd.Data, nil)
			}

			if stats, ok := forecasterStats[f.Raw][k]; ok {
				for sKey, sVal := range stats {
					v := float32(math.Round(float64(sVal*capacity)*100) / 100)
					sd.BandMap[idx][sKey] = &v
				}

				if sd.HasBands {
					for bIdx, pair := range activePairs {
						if bLow, hasLow := sd.BandMap[idx][pair.LowerName]; hasLow {
							if bUp, hasUp := sd.BandMap[idx][pair.UpperName]; hasUp {
								sd.Bands[bIdx].Lower[idx] = bLow
								diff := float32(math.Round(float64(*bUp-*bLow)*100) / 100)
								sd.Bands[bIdx].Diff[idx] = &diff
							}
						}
					}
				}
			}
		}

		allSeries = append(allSeries, sd)
	}

	for _, o := range observers {
		sd := SeriesData{Name: o.Name + " [Obs]", IsObservation: true}
		for _, k := range timeKeys {
			if val, ok := forecasterResults[o.Raw][k]; ok {
				v := float32(math.Round(float64(val*capacity)*100) / 100)
				sd.Data = append(sd.Data, &v)
			} else {
				sd.Data = append(sd.Data, nil)
			}
		}

		allSeries = append(allSeries, sd)
	}

	var avgFraction float32
	if count > 0 {
		avgFraction = totalFraction / float32(count)
	}

	return allSeries, labels, timeKeys, avgFraction
}

package ui

import (
	"compress/gzip"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

//go:embed templates/*.html static/*
var templateFiles embed.FS

var tpl *template.Template

func init() {
	funcs := template.FuncMap{
		"calcWatts": func(frac float32, cap uint64) float32 {
			return frac * float32(cap)
		},
		"formatCapacity": func(cap uint64) string {
			if cap >= 1_000_000 {
				return fmt.Sprintf("%.1fMW", float64(cap)/1_000_000.0)
			} else if cap >= 1_000 {
				return fmt.Sprintf("%.1fkW", float64(cap)/1_000.0)
			}

			return fmt.Sprintf("%dW", cap)
		},
		"toJSON": func(v interface{}) template.JS {
			b, _ := json.Marshal(v)
			return template.JS(b)
		},
	}

	var err error

	tpl, err = template.New("").Funcs(funcs).ParseFS(templateFiles, "templates/*.html")
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to parse UI templates")
	}
}

type UIClient struct {
	grpcClient pb.DataPlatformDataServiceClient
}

func NewUIClient(grpcTarget string) (*UIClient, error) {
	conn, err := grpc.NewClient(
		grpcTarget,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(uiMetadataUnaryInterceptor),
		grpc.WithChainStreamInterceptor(uiMetadataStreamInterceptor),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial gRPC target %s: %w", grpcTarget, err)
	}

	client := pb.NewDataPlatformDataServiceClient(conn)

	return &UIClient{
		grpcClient: client,
	}, nil
}

type traceKeyType struct{}

func getTraceID(ctx context.Context) string {
	if tid, ok := ctx.Value(traceKeyType{}).(string); ok && tid != "" {
		return tid
	}

	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

func uiMetadataUnaryInterceptor(
	ctx context.Context,
	method string,
	req, reply any,
	cc *grpc.ClientConn,
	invoker grpc.UnaryInvoker,
	opts ...grpc.CallOption,
) error {
	traceID := getTraceID(ctx)
	ctx = metadata.AppendToOutgoingContext(ctx, "traceid", traceID, "appid", "dp-ui")
	return invoker(ctx, method, req, reply, cc, opts...)
}

func uiMetadataStreamInterceptor(
	ctx context.Context,
	desc *grpc.StreamDesc,
	cc *grpc.ClientConn,
	method string,
	streamer grpc.Streamer,
	opts ...grpc.CallOption,
) (grpc.ClientStream, error) {
	traceID := getTraceID(ctx)
	ctx = metadata.AppendToOutgoingContext(ctx, "traceid", traceID, "appid", "dp-ui")
	return streamer(ctx, desc, cc, method, opts...)
}

type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func withGzip(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			next.ServeHTTP(w, r)
			return
		}

		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Vary", "Accept-Encoding")

		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzw := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		next.ServeHTTP(gzw, r)
	})
}

func withTraceID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceID := r.Header.Get("X-Request-Id")
		if traceID == "" {
			traceID = strings.ReplaceAll(uuid.New().String(), "-", "")
		}

		w.Header().Set("X-Request-Id", traceID)
		ctx := context.WithValue(r.Context(), traceKeyType{}, traceID)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (ui *UIClient) Start(port string) error {
	mux := http.NewServeMux()

	mux.Handle("/static/", http.FileServer(http.FS(templateFiles)))
	mux.HandleFunc("/", ui.handleIndex)
	mux.HandleFunc("/locations", ui.handleLocations)
	mux.HandleFunc("/components/selectors", ui.handleSelectors)
	mux.HandleFunc("/components/forecast", ui.handleForecast)
	mux.HandleFunc("/components/location_details", ui.handleLocationDetails)
	mux.HandleFunc("/components/location_edit", ui.handleLocationEdit)
	mux.HandleFunc("/api/dashboard/map-snapshot", ui.handleDashboardMapSnapshot)

	return http.ListenAndServe(port, withTraceID(withGzip(mux)))
}

func (ui *UIClient) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	data := struct {
		Country string
	}{
		Country: "uk",
	}

	err := tpl.ExecuteTemplate(w, "forecasts.html", data)
	if err != nil {
		log.Error().Err(err).Msg("Failed to execute forecasts template")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (ui *UIClient) handleSelectors(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	mode := r.URL.Query().Get("mode")

	var (
		locResp *pb.ListLocationsResponse
		fcResp  *pb.ListForecastersResponse
		obsResp *pb.ListObserversResponse
	)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		var locErr error

		locResp, locErr = ui.grpcClient.ListLocations(gCtx, &pb.ListLocationsRequest{})
		if locErr != nil {
			log.Error().Err(locErr).Msg("Failed to list locations")
		}

		return locErr
	})

	g.Go(func() error {
		var fcErr error

		fcResp, fcErr = ui.grpcClient.ListForecasters(gCtx, &pb.ListForecastersRequest{})
		if fcErr != nil {
			log.Error().Err(fcErr).Msg("Failed to list forecasters")
		}

		return fcErr
	})

	g.Go(func() error {
		var obsErr error

		obsResp, obsErr = ui.grpcClient.ListObservers(gCtx, &pb.ListObserversRequest{})
		if obsErr != nil {
			log.Error().Err(obsErr).Msg("Failed to list observers")
		}

		return obsErr
	})

	if err := g.Wait(); err != nil {
		log.Error().Err(err).Msg("Failed to list required resources for selectors")
		http.Error(
			w,
			fmt.Sprintf("Failed to list required resources: %v", err),
			http.StatusInternalServerError,
		)

		return
	}

	var dashboardCountry, defaultLocationUUID string
	if mode != "" {
		dashboardCountry = mode
		// find location uuid for country
		for _, loc := range locResp.GetLocations() {
			if strings.EqualFold(loc.GetLocationName(), dashboardCountry) {
				defaultLocationUUID = loc.GetLocationUuid()
				break
			}
		}
	}

	data := struct {
		Locations           []*pb.ListLocationsResponse_LocationSummary
		Forecasters         []*pb.Forecaster
		Observers           []*pb.ListObserversResponse_ObserverSummary
		DefaultTimeWindow   string
		DashboardCountry    string
		DefaultLocationUUID string
	}{
		Locations:   locResp.GetLocations(),
		Forecasters: fcResp.GetForecasters(),
		Observers:   obsResp.GetObservers(),
		DefaultTimeWindow: fmt.Sprintf("%s to %s",
			time.Now().UTC().Add(-48*time.Hour).Format("2006-01-02 15:04"),
			time.Now().UTC().Add(36*time.Hour).Format("2006-01-02 15:04"),
		),
		DashboardCountry:    dashboardCountry,
		DefaultLocationUUID: defaultLocationUUID,
	}

	err := tpl.ExecuteTemplate(w, "selectors.html", data)
	if err != nil {
		log.Error().Err(err).Msg("Failed to execute selectors template")
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

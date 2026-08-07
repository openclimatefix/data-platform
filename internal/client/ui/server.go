package ui

import (
	"bytes"
	"compress/gzip"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"net/http"
	"net/url"
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

// Defaults for the single dashboard view.
const (
	defaultDashboardLocationName = "uk"
	defaultForecasterNamePrefix  = "blend"
	defaultObserverNamePrimary   = "pvlive_in_day"
	defaultObserverNameSecondary = "pvlive_day_after"
)

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

func httpError(w http.ResponseWriter, r *http.Request, msg string, code int, err error) {
	if err != nil {
		log.Error().Err(err).Str("method", r.Method).Str("path", r.URL.Path).Msg(msg)
	} else {
		log.Warn().Str("method", r.Method).Str("path", r.URL.Path).Msg(msg)
	}
	http.Error(w, msg, code)
}

func render(w http.ResponseWriter, r *http.Request, name string, data any) {
	var buf bytes.Buffer
	if err := tpl.ExecuteTemplate(&buf, name, data); err != nil {
		httpError(w, r, "Template rendering failed", http.StatusInternalServerError, err)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}

func (ui *UIClient) Start(port string) error {
	mux := http.NewServeMux()

	staticFS, err := fs.Sub(templateFiles, "static")
	if err != nil {
		return err
	}
	staticHandler := http.StripPrefix("/static/", http.FileServer(http.FS(staticFS)))
	mux.Handle("/static/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=0, must-revalidate")
		staticHandler.ServeHTTP(w, r)
	}))

	mux.HandleFunc("/", ui.handleIndex)
	mux.HandleFunc("/locations", ui.handleLocations)
	mux.HandleFunc("/components/selectors", ui.handleSelectors)
	mux.HandleFunc("/components/forecast", ui.handleForecast)
	mux.HandleFunc("/components/location_details", ui.handleLocationDetails)
	mux.HandleFunc("/components/location_edit", ui.handleLocationEdit)
	mux.HandleFunc("/api/dashboard/map-snapshot", ui.handleDashboardMapSnapshot)

	srv := &http.Server{
		Addr:         port,
		Handler:      withTraceID(withGzip(mux)),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	return srv.ListenAndServe()
}

func (ui *UIClient) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	if r.URL.RawQuery == "" {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		g, gCtx := errgroup.WithContext(ctx)
		var locResp *pb.ListLocationsResponse
		var fcResp *pb.ListForecastersResponse

		g.Go(func() error {
			var err error
			locResp, err = ui.grpcClient.ListLocations(gCtx, &pb.ListLocationsRequest{})
			return err
		})

		g.Go(func() error {
			var err error
			fcResp, err = ui.grpcClient.ListForecasters(gCtx, &pb.ListForecastersRequest{})
			return err
		})

		if err := g.Wait(); err == nil {
			var defaultLocationUUID string
			for _, loc := range locResp.GetLocations() {
				if strings.EqualFold(loc.GetLocationName(), defaultDashboardLocationName) {
					defaultLocationUUID = loc.GetLocationUuid()
					break
				}
			}
			
			// Fallback if "uk" doesn't exist (e.g. dummy dataset)
			if defaultLocationUUID == "" && len(locResp.GetLocations()) > 0 {
				defaultLocationUUID = locResp.GetLocations()[0].GetLocationUuid()
			}

			var defaultFcVersion string
			for _, fc := range fcResp.GetForecasters() {
				if strings.HasPrefix(fc.GetForecasterName(), defaultForecasterNamePrefix) {
					defaultFcVersion = fc.GetForecasterVersion()
					break
				}
			}
			if defaultFcVersion == "" {
				defaultFcVersion = "unknown"
			}

			if defaultLocationUUID != "" {
				now := time.Now().UTC()
				v := url.Values{}
				v.Set("location_uuid", defaultLocationUUID)
				v.Set("energy_source", "1")
				v.Set("start", now.Add(-48*time.Hour).Format("2006-01-02 15:04"))
				v.Set("end", now.Add(36*time.Hour).Format("2006-01-02 15:04"))
				v.Add("forecaster", defaultForecasterNamePrefix+"|"+defaultFcVersion+"|0")
				v.Add("observer", defaultObserverNamePrimary)
				v.Add("observer", defaultObserverNameSecondary)

				http.Redirect(w, r, "/?"+v.Encode(), http.StatusFound)
				return
			}
		}
	}

	data := struct{ Query template.URL }{
		Query: template.URL(r.URL.RawQuery),
	}
	render(w, r, "forecasts.html", data)
}

func (ui *UIClient) handleSelectors(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var (
		locResp *pb.ListLocationsResponse
		fcResp  *pb.ListForecastersResponse
		obsResp *pb.ListObserversResponse
	)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		locResp, _ = ui.grpcClient.ListLocations(gCtx, &pb.ListLocationsRequest{})
		return nil
	})

	g.Go(func() error {
		fcResp, _ = ui.grpcClient.ListForecasters(gCtx, &pb.ListForecastersRequest{})
		return nil
	})

	g.Go(func() error {
		obsResp, _ = ui.grpcClient.ListObservers(gCtx, &pb.ListObserversRequest{})
		return nil
	})

	_ = g.Wait()

	var locLabel string
	locUUID := r.URL.Query().Get("location_uuid")

	for _, loc := range locResp.GetLocations() {
		if loc.GetLocationUuid() == locUUID {
			capStr := ""
			if capWatts := loc.GetEffectiveCapacityWatts(); capWatts >= 1_000_000 {
				capStr = fmt.Sprintf("%.1fMW", float64(capWatts)/1_000_000.0)
			} else if capWatts >= 1_000 {
				capStr = fmt.Sprintf("%.1fkW", float64(capWatts)/1_000.0)
			} else {
				capStr = fmt.Sprintf("%dW", capWatts)
			}
			locLabel = fmt.Sprintf("%s (%s, %s)", loc.GetLocationName(), loc.GetLocationType().String(), capStr)
			break
		}
	}

	esRaw := r.URL.Query().Get("energy_source")
	startRaw := r.URL.Query().Get("start")
	endRaw := r.URL.Query().Get("end")

	timeWindow := ""
	if startRaw != "" && endRaw != "" {
		timeWindow = startRaw + " to " + endRaw
	} else {
		now := time.Now().UTC()
		timeWindow = fmt.Sprintf("%s to %s",
			now.Add(-48*time.Hour).Format("2006-01-02 15:04"),
			now.Add(36*time.Hour).Format("2006-01-02 15:04"),
		)
	}

	type selectedSource struct {
		Type  string
		Value string
		Label string
	}
	var selectedSources []selectedSource

	for _, fRaw := range r.URL.Query()["forecaster"] {
		parts := strings.Split(fRaw, "|")
		if len(parts) >= 1 {
			// Find nice label
			label := parts[0]
			for _, f := range fcResp.GetForecasters() {
				if f.GetForecasterName() == parts[0] {
					label = f.GetForecasterName() + " (v" + f.GetForecasterVersion() + ")"
					break
				}
			}
			if len(parts) == 3 {
				label += " @ " + parts[2] + "m"
			} else {
				label += " @ 0m"
			}
			selectedSources = append(selectedSources, selectedSource{
				Type:  "forecaster",
				Value: fRaw,
				Label: label,
			})
		}
	}

	for _, oRaw := range r.URL.Query()["observer"] {
		selectedSources = append(selectedSources, selectedSource{
			Type:  "observer",
			Value: oRaw,
			Label: oRaw + " [Observer]",
		})
	}

	data := struct {
		Locations          []*pb.ListLocationsResponse_LocationSummary
		Forecasters        []*pb.Forecaster
		Observers          []*pb.ListObserversResponse_ObserverSummary
		EnergySources      []energySourceOption
		SelectedEnergy     string
		TimeWindow         string
		LocationUUID       string
		LocationLabel      string
		SelectedSources    []selectedSource
	}{
		Locations:          locResp.GetLocations(),
		Forecasters:        fcResp.GetForecasters(),
		Observers:          obsResp.GetObservers(),
		EnergySources:      getEnergySourceOptions(),
		SelectedEnergy:     esRaw,
		TimeWindow:         timeWindow,
		LocationUUID:       locUUID,
		LocationLabel:      locLabel,
		SelectedSources:    selectedSources,
	}

	render(w, r, "selectors.html", data)
}

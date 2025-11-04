package main

import (
	"fmt"
	"net"
	"slices"
	"strings"
	_ "embed"

	"buf.build/go/protovalidate"
	protovalidate_ix "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"github.com/gurkankaymak/hocon"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // GRPC will automatically negotiate and use gzip if the client supports it.
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
	ix "github.com/openclimatefix/data-platform/internal/interceptors"
	dbdy "github.com/openclimatefix/data-platform/internal/server/dummy"
	dbpg "github.com/openclimatefix/data-platform/internal/server/postgres"
)


//go:embed server.conf
var confString string

func main() {
	conf, err := hocon.ParseString(confString)
	if err != nil {
		log.Fatal().Err(err).Msg("error parsing configuration")
	}
	log.Debug().RawJSON("config", []byte(conf.String())).Msg("loaded configuration")

	// Setup logging
	logLevel, err := zerolog.ParseLevel(conf.GetString("loglevel"))
	if err != nil {
		log.Fatal().Err(err).Msgf("invalid log level: %s", conf.GetString("loglevel"))
	}
	zerolog.SetGlobalLevel(logLevel)

	// Create a TCP listener on the specified port
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", conf.GetInt("port")))
	if err != nil {
		log.Fatal().Err(err).Msgf("net.Listen({tcp: %d})", conf.GetInt("port"))
	}

	// Choose the server implementation based on the environment
	databaseUrl := strings.ToLower(conf.GetString("dburl"))

	var (
		dataServerImpl  pb.DataPlatformDataServiceServer
		adminServerImpl pb.DataPlatformAdministrationServiceServer
		s               *grpc.Server
	)
	
	// Create a validator to use with protovalidate interceptor
	validator, err := protovalidate.New()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to create validator")
	}

	if slices.Contains([]string{"", "dummy", "fake"}, strings.ToLower(databaseUrl)) {
		log.Warn().Msg("running in test mode with fake data. Not for production use")

		dataServerImpl = dbdy.NewDataPlatformDataServerImpl()
		adminServerImpl = dbdy.NewDataPlatformAdministrationServiceServerImpl()

		// For a dummy-backed server, just validate requests
		s = grpc.NewServer(
			grpc.ChainUnaryInterceptor(
				grpc.UnaryServerInterceptor(protovalidate_ix.UnaryServerInterceptor(validator)),
			),
		)
	} else if strings.HasPrefix(databaseUrl, "postgres") && strings.Contains(databaseUrl, "://") {
		log.Debug().Str("type", "postgresql").Msg("Connecting to database backend")

		logInterceptor := ix.NewLoggingInterceptor()
		txInterceptor := ix.NewTransactionInterceptor(databaseUrl, dbpg.Migrations)
		dataServerImpl = dbpg.NewDataPlatformDataServiceServerImpl()
		adminServerImpl = dbpg.NewDataPlatformAdministrationServiceServerImpl()

		// For a postgres-backed server, validate requests and manage database transactions
		s = grpc.NewServer(
			grpc.ChainUnaryInterceptor(
				grpc.UnaryServerInterceptor(protovalidate_ix.UnaryServerInterceptor(validator)),
				grpc.UnaryServerInterceptor(logInterceptor.UnaryServerInterceptor),
				grpc.UnaryServerInterceptor(txInterceptor.UnaryServerInterceptor),
			),
			grpc.ChainStreamInterceptor(
				grpc.StreamServerInterceptor(protovalidate_ix.StreamServerInterceptor(validator)),
				grpc.StreamServerInterceptor(logInterceptor.StreamServerInterceptor),
				grpc.StreamServerInterceptor(txInterceptor.StreamServerInterceptor),
			),
		)
	} else {
		log.Fatal().Str("url", databaseUrl).Msg("unsupported DATABASE_URL format")
	}

	// Create the GRPC server
	// * Add an interceptor for request validation
	pb.RegisterDataPlatformDataServiceServer(s, dataServerImpl)
	pb.RegisterDataPlatformAdministrationServiceServer(s, adminServerImpl)
	grpc_health_v1.RegisterHealthServer(s, health.NewServer())
	reflection.Register(s)

	log.Info().Msgf("listening on :%d", conf.GetInt("port"))
	_ = s.Serve(lis) // If this errors, we want it to panic! It's fundamental
}

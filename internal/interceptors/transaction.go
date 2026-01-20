// This module defines custom GRPC interceptors.
package interceptors

import (
	"context"
	"embed"

	"github.com/jackc/pgx/v5/stdlib"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Establish a private type for the context key to avoid collisions.
type txKey struct{}

// GetTxFromContext retrieves the transaction from the context.
// It panics if the transaction is not found, as presumably anything calling this function
// expects it to be there, and we shouldn't fail silently in that case.
func GetTxFromContext(ctx context.Context) pgx.Tx {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	if !ok {
		log.Fatal().Msg(
			"Transaction not injected to context. Ensure the TransactionInjector interceptor " +
				"is included in the server's interceptor chain.",
		)
	}

	return tx
}

// transactionInterceptorBuilder defines an interface to inject transactions into the request
// context of gRPC handlers.
// I've decided this is alright to do because a transaction is fundamentally scoped to the request.
type transactionInterceptorBuilder struct {
	pool *pgxpool.Pool
}

func NewTransactionInterceptor(
	connString string,
	migrations embed.FS,
) *transactionInterceptorBuilder {
	pool, err := pgxpool.New(
		context.Background(), connString,
	)
	if err != nil {
		log.Fatal().Msg("Unable to connect to database. Ensure DATABASE_URL is set correctly")
	}

	goose.SetBaseFS(migrations)
	goose.SetLogger(goose.NopLogger())

	_ = goose.SetDialect("postgres")

	db := stdlib.OpenDBFromPool(pool)

	err = goose.Up(db, "sql/migrations")
	if err != nil {
		log.Fatal().Msgf("Unable to apply migrations: %v", err)
	}

	err = db.Close()
	if err != nil {
		log.Fatal().Msgf("Unable to close database connection: %v", err)
	}

	log.Debug().Msg("Completed migrations")

	return &transactionInterceptorBuilder{pool: pool}
}

// UnaryServerInterceptor is the gRPC interceptor for handling transactions.
func (ti *transactionInterceptorBuilder) UnaryServerInterceptor(
	ctx context.Context,
	req any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	l := zerolog.Ctx(ctx)
	// Establish a transaction with the database.
	tx, err := ti.pool.Begin(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, "Error communicating with backend")
	}

	// If an error occurred, rollback the transaction.
	defer func() {
		if err != nil {
			l.Debug().Msg("rolling back transaction")
			_ = tx.Rollback(ctx)
		} else {
			_ = tx.Commit(ctx)
		}
	}()

	// Create a new context with the transaction injected. Returned errors will trigger the defer.
	txCtx := context.WithValue(ctx, txKey{}, tx)

	// Call the original RPC handler with the new context.
	resp, err := handler(txCtx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// StreamServerInterceptor is the gRPC interceptor for handling transactions in streams.
func (ti *transactionInterceptorBuilder) StreamServerInterceptor(
	srv any,
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) error {
	l := zerolog.Ctx(ss.Context())
	// Establish a transaction with the database.
	tx, err := ti.pool.Begin(ss.Context())
	if err != nil {
		return status.Error(codes.Internal, "Error communicating with backend")
	}

	// If an error occurred, rollback the transaction.
	defer func() {
		if err != nil {
			l.Debug().Msg("rolling back transaction")
			_ = tx.Rollback(ss.Context())
		} else {
			_ = tx.Commit(ss.Context())
		}
	}()

	// Create a new context with the transaction injected. Returned errors will trigger the defer.
	txCtx := context.WithValue(ss.Context(), txKey{}, tx)

	// Wrap the ServerStream to inject our new context.
	err = handler(srv, &wrappedStream{ServerStream: ss, newCtx: txCtx})
	if err != nil {
		return err
	}

	return nil
}

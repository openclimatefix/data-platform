// Package postgres defines server implementation for the DataPlatformServiceServer.
// This implementation is backed by a PostgreSQL database.
//
// Functions and structs for connecting to the database are generated from SQL using
// the sqlc library, whilst the Server interface that is being implemented comes from
// the top-level proto definitions.
package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

// --- Server Implementation ----------------------------------------------------------------------

func NewDataPlatformAdministrationServiceServerImpl(
	pool *pgxpool.Pool,
) *DataPlatformAdministrationServiceServerImpl {
	return &DataPlatformAdministrationServiceServerImpl{
		pool: pool,
	}
}

type DataPlatformAdministrationServiceServerImpl struct {
	pool *pgxpool.Pool
}

// CreateLocationPolicyGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) CreateLocationPolicyGroup(
	context.Context,
	*pb.CreateLocationPolicyGroupRequest,
) (*pb.CreateLocationPolicyGroupResponse, error) {
	panic("unimplemented")
}

// CreateOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) CreateOrganisation(
	context.Context,
	*pb.CreateOrganisationRequest,
) (*pb.CreateOrganisationResponse, error) {
	panic("unimplemented")
}

// CreateUser implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) CreateUser(
	context.Context,
	*pb.CreateUserRequest,
) (*pb.CreateUserResponse, error) {
	panic("unimplemented")
}

// DeleteLocationPolicyGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) DeleteLocationPolicyGroup(
	context.Context,
	*pb.DeleteLocationPolicyGroupRequest,
) (*pb.DeleteLocationPolicyGroupResponse, error) {
	panic("unimplemented")
}

// DeleteOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) DeleteOrganisation(
	context.Context,
	*pb.DeleteOrganisationRequest,
) (*pb.DeleteOrganisationResponse, error) {
	panic("unimplemented")
}

// DeleteUser implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) DeleteUser(
	context.Context,
	*pb.DeleteUserRequest,
) (*pb.DeleteUserResponse, error) {
	panic("unimplemented")
}

// GetLocationPolicyGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) GetLocationPolicyGroup(
	context.Context,
	*pb.GetLocationPolicyGroupRequest,
) (*pb.GetLocationPolicyGroupResponse, error) {
	panic("unimplemented")
}

// GetOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) GetOrganisation(
	context.Context,
	*pb.GetOrganisationRequest,
) (*pb.GetOrganisationResponse, error) {
	panic("unimplemented")
}

// GetUser implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) GetUser(
	context.Context,
	*pb.GetUserRequest,
) (*pb.GetUserResponse, error) {
	panic("unimplemented")
}

// UpdateLocationPolicyGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) UpdateLocationPolicyGroup(
	context.Context,
	*pb.UpdateLocationPolicyGroupRequest,
) (*pb.UpdateLocationPolicyGroupResponse, error) {
	panic("unimplemented")
}

// UpdateOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) UpdateOrganisation(
	context.Context,
	*pb.UpdateOrganisationRequest,
) (*pb.UpdateOrganisationResponse, error) {
	panic("unimplemented")
}

// Compile-time check to ensure the interface is implemented fully.
var _ pb.DataPlatformAdministrationServiceServer = (*DataPlatformAdministrationServiceServerImpl)(
	nil,
)

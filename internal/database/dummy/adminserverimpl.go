// Package postgres defines a server implementation for the DataPlatformServiceServer.
// This implementation is backed by a PostgreSQL database.
//
// Functions and structs for connecting to the database are generated from SQL using
// the sqlc library, whilst the Server interface that is being implemented comes from
// the top-level proto definitions.
package dummy

import (
	"context"

	"github.com/google/uuid"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

// --- Server Implementation ----------------------------------------------------------------------

func NewDataPlatformAdministrationServiceServerImpl() *DataPlatformAdministrationServiceServerImpl {
	return &DataPlatformAdministrationServiceServerImpl{}
}

type DataPlatformAdministrationServiceServerImpl struct{}

func (d *DataPlatformAdministrationServiceServerImpl) CreateLocationPolicyGroup(
	ctx context.Context,
	req *pb.CreateLocationPolicyGroupRequest,
) (*pb.CreateLocationPolicyGroupResponse, error) {
	return &pb.CreateLocationPolicyGroupResponse{
		LocationPolicyGroupId: uuid.New().String(),
		Name:                  req.Name,
	}, nil
}

// CreateOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) CreateOrganisation(
	ctx context.Context,
	req *pb.CreateOrganisationRequest,
) (*pb.CreateOrganisationResponse, error) {
	return &pb.CreateOrganisationResponse{
		OrgId:   uuid.New().String(),
		OrgName: req.OrgName,
	}, nil
}

// CreateUser implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) CreateUser(
	context.Context,
	*pb.CreateUserRequest,
) (*pb.CreateUserResponse, error) {
	return &pb.CreateUserResponse{
		UserId: uuid.New().String(),
	}, nil
}

// DeleteLocationPolicyGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) DeleteLocationPolicyGroup(
	context.Context,
	*pb.DeleteLocationPolicyGroupRequest,
) (*pb.DeleteLocationPolicyGroupResponse, error) {
	return &pb.DeleteLocationPolicyGroupResponse{}, nil
}

// DeleteOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) DeleteOrganisation(
	context.Context,
	*pb.DeleteOrganisationRequest,
) (*pb.DeleteOrganisationResponse, error) {
	return &pb.DeleteOrganisationResponse{}, nil
}

// DeleteUser implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) DeleteUser(
	context.Context,
	*pb.DeleteUserRequest,
) (*pb.DeleteUserResponse, error) {
	return &pb.DeleteUserResponse{}, nil
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

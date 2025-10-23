// Package postgres defines a server implementation for the DataPlatformServiceServer.
// This implementation is backed by a PostgreSQL database.
//
// Functions and structs for connecting to the database are generated from SQL using
// the sqlc library, whilst the Server interface that is being implemented comes from
// the top-level proto definitions.
package dummy

import (
	"context"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

// --- Server Implementation ----------------------------------------------------------------------

func NewDataPlatformAdministrationServiceServerImpl() *DataPlatformAdministrationServiceServerImpl {
	return &DataPlatformAdministrationServiceServerImpl{}
}

type DataPlatformAdministrationServiceServerImpl struct{}

// CheckUserLocationAccess implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) CheckUserLocationAccess(
	context.Context,
	*pb.CheckUserLocationAccessRequest,
) (*pb.CheckUserLocationAccessResponse, error) {
	panic("unimplemented")
}

// ListUserLocations implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) ListUserLocations(
	context.Context,
	*pb.ListUserLocationsRequest,
) (*pb.ListUserLocationsResponse, error) {
	panic("unimplemented")
}

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
	ctx context.Context,
	req *pb.GetLocationPolicyGroupRequest,
) (*pb.GetLocationPolicyGroupResponse, error) {
	return &pb.GetLocationPolicyGroupResponse{
		LocationPolicyGroupId: req.LocationPolicyGroupId,
		Name:                  "Dummy Location Policy Group",
		LocationPolicies: []*pb.CreateLocationPolicyGroupRequest_LocationPolicy{
			{
				LocationId:   uuid.New().String(),
				EnergySource: pb.EnergySource_SOLAR,
				Scope:        "OWNER",
			},
		},
	}, nil
}

// GetOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) GetOrganisation(
	ctx context.Context,
	req *pb.GetOrganisationRequest,
) (*pb.GetOrganisationResponse, error) {
	return &pb.GetOrganisationResponse{
		OrgId:                uuid.New().String(),
		OrgName:              req.OrgName,
		Metadata:             &structpb.Struct{},
		CreatedAt:            timestamppb.New(time.Now().UTC()),
		LocationPolicyGroups: []string{"DUMMY_POLICY_GROUP_ID_1", "DUMMY_POLICY_GROUP_ID_2"},
		UserOauthIds:         []string{uuid.New().String()},
	}, nil
}

// GetUser implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) GetUser(
	ctx context.Context,
	req *pb.GetUserRequest,
) (*pb.GetUserResponse, error) {
	return &pb.GetUserResponse{
		UserId:               uuid.New().String(),
		OauthId:              req.OauthId,
		Organisation:         "Dummy Organisation",
		LocationPolicyGroups: []string{"DUMMY_POLICY_GROUP_ID_1", "DUMMY_POLICY_GROUP_ID_2"},
		CreatedAt:            timestamppb.New(time.Now().UTC()),
		Metadata:             &structpb.Struct{},
	}, nil
}

// UpdateLocationPolicyGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) UpdateLocationPolicyGroup(
	ctx context.Context,
	req *pb.UpdateLocationPolicyGroupRequest,
) (*pb.UpdateLocationPolicyGroupResponse, error) {
	return &pb.UpdateLocationPolicyGroupResponse{
		LocationPolicyGroupId: req.LocationPolicyGroupId,
		Name:                  "Dummy Location Policy Group",
		LocationPolicies:      req.LocationPolicies,
	}, nil
}

// UpdateOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) UpdateOrganisation(
	ctx context.Context,
	req *pb.UpdateOrganisationRequest,
) (*pb.UpdateOrganisationResponse, error) {
	return &pb.UpdateOrganisationResponse{
		OrgId:                uuid.New().String(),
		OrgName:              req.NewName,
		Metadata:             req.Metadata,
		CreatedAt:            timestamppb.New(time.Now().UTC()),
		LocationPolicyGroups: req.LocationPolicyGroupIds,
		UserOauthIds:         []string{uuid.New().String()},
	}, nil
}

// Compile-time check to ensure the interface is implemented fully.
var _ pb.DataPlatformAdministrationServiceServer = (*DataPlatformAdministrationServiceServerImpl)(
	nil,
)

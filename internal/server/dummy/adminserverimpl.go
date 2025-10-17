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

func (d *DataPlatformAdministrationServiceServerImpl) DeleteOrganisation(
	ctx context.Context,
	req *pb.DeleteOrganisationRequest,
) (*pb.DeleteOrganisationResponse, error) {
	return &pb.DeleteOrganisationResponse{}, nil
}

// CreateLocationPolicyGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) CreateLocationPolicyGroup(
	ctx context.Context,
	req *pb.CreateLocationPolicyGroupRequest,
) (*pb.CreateLocationPolicyGroupResponse, error) {
	return &pb.CreateLocationPolicyGroupResponse{
		LocationPolicyGroupId: uuid.New().String(),
		Name:                  req.Name,
	}, nil
}

// GetLocationPolicyGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) GetLocationPolicyGroup(
	ctx context.Context,
	req *pb.GetLocationPolicyGroupRequest,
) (*pb.GetLocationPolicyGroupResponse, error) {
	return &pb.GetLocationPolicyGroupResponse{
		LocationPolicyGroupId: uuid.New().String(),
		Name:                  "Dummy Location Policy Group",
		LocationPolicies: []*pb.LocationPolicy{
			{
				LocationId:   uuid.New().String(),
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				Permission:   pb.Permission_PERMISSION_READ,
			},
		},
	}, nil
}

// AddLocationPoliciesToGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) AddLocationPoliciesToGroup(
	context.Context,
	*pb.AddLocationPoliciesToGroupRequest,
) (*pb.AddLocationPoliciesToGroupResponse, error) {
	return &pb.AddLocationPoliciesToGroupResponse{}, nil
}

// RemoveLocationPoliciesFromGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) RemoveLocationPoliciesFromGroup(
	context.Context,
	*pb.RemoveLocationPoliciesFromGroupRequest,
) (*pb.RemoveLocationPoliciesFromGroupResponse, error) {
	return &pb.RemoveLocationPoliciesFromGroupResponse{}, nil
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

// CreateUser implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) CreateUser(
	ctx context.Context,
	req *pb.CreateUserRequest,
) (*pb.CreateUserResponse, error) {
	return &pb.CreateUserResponse{
		UserId:  uuid.New().String(),
		OauthId: req.OauthId,
	}, nil
}

// DeleteUser implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) DeleteUser(
	context.Context,
	*pb.DeleteUserRequest,
) (*pb.DeleteUserResponse, error) {
	return &pb.DeleteUserResponse{}, nil
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

// AddLocationPolicyGroupToOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) AddLocationPolicyGroupToOrganisation(
	context.Context,
	*pb.AddLocationPolicyGroupToOrganisationRequest,
) (*pb.AddLocationPolicyGroupToOrganisationResponse, error) {
	return &pb.AddLocationPolicyGroupToOrganisationResponse{}, nil
}

// AddUserToOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) AddUserToOrganisation(
	context.Context,
	*pb.AddUserToOrganisationRequest,
) (*pb.AddUserToOrganisationResponse, error) {
	return &pb.AddUserToOrganisationResponse{}, nil
}

// RemoveLocationPolicyGroupFromOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) RemoveLocationPolicyGroupFromOrganisation(
	context.Context,
	*pb.RemoveLocationPolicyGroupFromOrganisationRequest,
) (*pb.RemoveLocationPolicyGroupFromOrganisationResponse, error) {
	return &pb.RemoveLocationPolicyGroupFromOrganisationResponse{}, nil
}

// RemoveUserFromOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) RemoveUserFromOrganisation(
	context.Context,
	*pb.RemoveUserFromOrganisationRequest,
) (*pb.RemoveUserFromOrganisationResponse, error) {
	return &pb.RemoveUserFromOrganisationResponse{}, nil
}

// Compile-time check to ensure the interface is implemented fully.
var _ pb.DataPlatformAdministrationServiceServer = (*DataPlatformAdministrationServiceServerImpl)(
	nil,
)

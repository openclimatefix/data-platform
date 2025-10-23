// Package postgres defines server implementation for the DataPlatformServiceServer.
// This implementation is backed by a PostgreSQL database.
//
// Functions and structs for connecting to the database are generated from SQL using
// the sqlc library, whilst the Server interface that is being implemented comes from
// the top-level proto definitions.
//
// NOTE: I am happy to use MustParse for the UUID handling here as the validation middleware
// is handling uuid checks upstream.
package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
	ix "github.com/openclimatefix/data-platform/internal/interceptors"
	db "github.com/openclimatefix/data-platform/internal/server/postgres/gen"
)

// --- Server Implementation ----------------------------------------------------------------------

func NewDataPlatformAdministrationServiceServerImpl() *DataPlatformAdministrationServiceServerImpl {
	return &DataPlatformAdministrationServiceServerImpl{}
}

// DataPlatformAdministrationServiceServerImpl implements the pb.DataPlatformDataServiceServer interface.
// It requires the database transaction for the request to be set in the context.
type DataPlatformAdministrationServiceServerImpl struct{}

func (d *DataPlatformAdministrationServiceServerImpl) CheckUserLocationAccess(
	ctx context.Context,
	re *pb.CheckUserLocationAccessRequest,
) (*pb.CheckUserLocationAccessResponse, error) {
	panic("unimplemented")
}

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
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	clpgParams := db.CreateLocationPolicyGroupParams{
		LocationPolicyGroupName: req.Name,
	}

	dbGroup, err := querier.CreateLocationPolicyGroup(ctx, clpgParams)
	if err != nil {
		l.Error().Err(err).Msg("Error creating location policy group")

		return nil, status.Error(
			codes.Internal,
			"Encountered database error",
		)
	}

	return &pb.CreateLocationPolicyGroupResponse{
		LocationPolicyGroupId: dbGroup.LocationPolicyGroupUuid.String(),
		Name:                  dbGroup.LocationPolicyGroupName,
	}, nil
}

// CreateOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) CreateOrganisation(
	ctx context.Context,
	req *pb.CreateOrganisationRequest,
) (*pb.CreateOrganisationResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	metadata, err := req.Metadata.MarshalJSON()
	if err != nil {
		l.Err(err).Msgf("req.Metadata.MarshalJSON()")

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid metadata. Ensure object is JSON serializable.",
		)
	}

	coprms := db.CreateOrgParams{
		OrgName:  req.OrgName,
		Metadata: metadata,
	}

	dbOrg, err := querier.CreateOrg(ctx, coprms)
	if err != nil {
		l.Error().Err(err).Msgf("querier.CreateOrg(%+v)", coprms)

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid Organisation request. Ensure name is unique and metadata is valid JSON.",
		)
	}

	return &pb.CreateOrganisationResponse{
		OrgId:   dbOrg.OrgUuid.String(),
		OrgName: dbOrg.OrgName,
	}, nil
}

// CreateUser implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) CreateUser(
	ctx context.Context,
	req *pb.CreateUserRequest,
) (*pb.CreateUserResponse, error) {
	l := zerolog.Ctx(ctx)

	querier := db.New(ix.GetTxFromContext(ctx))

	goParams := db.GetOrgByNameParams{
		OrgName: req.Organisation,
	}

	dbOrg, err := querier.GetOrgByName(ctx, goParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.GetOrgByName(%+v)", goParams)

		return nil, status.Errorf(
			codes.NotFound,
			"Organisation with name '%s' not found. Choose an existing organisation, "+
				"or create a new organisation before adding users to it.",
			req.Organisation,
		)
	}

	metadata, err := req.Metadata.MarshalJSON()
	if err != nil {
		l.Err(err).Msgf("req.Metadata.MarshalJSON()")

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid metadata. Ensure object is JSON serializable.",
		)
	}

	cuParams := db.CreateUserParams{
		OrgUuid:  dbOrg.OrgUuid,
		OauthID:  req.OauthId,
		Metadata: metadata,
	}

	dbUser, err := querier.CreateUser(ctx, cuParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.CreateUser(%+v)", cuParams)

		return nil, status.Error(
			codes.InvalidArgument,
			"Invalid User request. Ensure OAuth ID is of the correct format and  metadata is valid JSON.",
		)
	}

	return &pb.CreateUserResponse{
		UserId: dbUser.UserUuid.String(),
	}, nil
}

// DeleteLocationPolicyGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) DeleteLocationPolicyGroup(
	ctx context.Context,
	req *pb.DeleteLocationPolicyGroupRequest,
) (*pb.DeleteLocationPolicyGroupResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	lpgUuid := uuid.MustParse(req.LocationPolicyGroupId)
	dlpgParams := db.DeleteLocationPolicyGroupParams{
		LocationPolicyGroupUuid: lpgUuid,
	}

	err := querier.DeleteLocationPolicyGroup(ctx, dlpgParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.DeleteLocationPolicyGroup(%+v)", dlpgParams)

		return nil, status.Errorf(
			codes.NotFound,
			"Location policy group with ID '%s' not found",
			req.LocationPolicyGroupId,
		)
	}

	return &pb.DeleteLocationPolicyGroupResponse{}, nil
}

// DeleteOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) DeleteOrganisation(
	ctx context.Context,
	req *pb.DeleteOrganisationRequest,
) (*pb.DeleteOrganisationResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	orgUuid := uuid.MustParse(req.OrgId)
	doParams := db.DeleteOrgParams{
		OrgUuid: orgUuid,
	}

	err := querier.DeleteOrg(ctx, doParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.DeleteOrg(%+v)", doParams)

		return nil, status.Errorf(
			codes.NotFound,
			"Organisation with ID '%s' not found",
			req.OrgId,
		)
	}

	return &pb.DeleteOrganisationResponse{}, nil
}

// DeleteUser implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) DeleteUser(
	ctx context.Context,
	req *pb.DeleteUserRequest,
) (*pb.DeleteUserResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	userUuid := uuid.MustParse(req.UserId)
	duParams := db.DeleteUserParams{
		UserUuid: userUuid,
	}

	err := querier.DeleteUser(ctx, duParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.DeleteUser(%+v)", duParams)

		return nil, status.Errorf(
			codes.NotFound,
			"User with ID '%s' not found",
			req.UserId,
		)
	}

	return &pb.DeleteUserResponse{}, nil
}

// GetLocationPolicyGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) GetLocationPolicyGroup(
	ctx context.Context,
	req *pb.GetLocationPolicyGroupRequest,
) (*pb.GetLocationPolicyGroupResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	lpgUuid := uuid.MustParse(req.LocationPolicyGroupId)
	glpgParams := db.GetLocationPolicyGroupByUUIDParams{
		LocationPolicyGroupUuid: lpgUuid,
	}

	dbGroup, err := querier.GetLocationPolicyGroupByUUID(ctx, glpgParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.GetLocationPolicyGroupByUUID(%+v)", glpgParams)

		return nil, status.Errorf(
			codes.NotFound,
			"Location policy group with ID '%s' not found",
			req.LocationPolicyGroupId,
		)
	}

	llpParams := db.ListLocationPoliciesByGroupParams{
		LocationPolicyGroupUuid: lpgUuid,
	}

	dbPolicies, err := querier.ListLocationPoliciesByGroup(ctx, llpParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.ListLocationPoliciesByGroup(%+v)", llpParams)

		return nil, status.Errorf(
			codes.NotFound,
			"No location policy group found with ID '%s'",
			req.LocationPolicyGroupId,
		)
	}

	policies := make([]*pb.CreateLocationPolicyGroupRequest_LocationPolicy, len(dbPolicies))
	for i, p := range dbPolicies {
		policies[i] = &pb.CreateLocationPolicyGroupRequest_LocationPolicy{
			LocationId:   p.LocationUuid.String(),
			EnergySource: pb.EnergySource(pb.EnergySource_value[p.SourceTypeName]),
			Scope:        p.RoleName,
		}
	}

	return &pb.GetLocationPolicyGroupResponse{
		LocationPolicyGroupId: dbGroup.LocationPolicyGroupUuid.String(),
		Name:                  dbGroup.LocationPolicyGroupName,
		LocationPolicies:      policies,
	}, nil
}

// GetOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) GetOrganisation(
	ctx context.Context,
	req *pb.GetOrganisationRequest,
) (*pb.GetOrganisationResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	goParams := db.GetOrgByNameParams{
		OrgName: req.OrgName,
	}

	dbOrg, err := querier.GetOrgByName(ctx, goParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.GetOrgByName(%+v)", goParams)

		return nil, status.Errorf(
			codes.NotFound,
			"Organisation with name '%s' not found",
			req.OrgName,
		)
	}

	metadata, err := jsonbToStruct(dbOrg.Metadata)
	if err != nil {
		l.Error().Err(err).Msgf("jsonbToStruct(%s)", dbOrg.Metadata)

		return nil, status.Errorf(
			codes.Internal,
			"Error parsing metadata for organisation with name '%s'",
			req.OrgName,
		)
	}

	return &pb.GetOrganisationResponse{
		OrgId:                dbOrg.OrgUuid.String(),
		OrgName:              dbOrg.OrgName,
		Metadata:             metadata,
		CreatedAt:            timestamppb.New(dbOrg.CreatedAtUtc.Time),
		LocationPolicyGroups: dbOrg.LocationPolicyGroupNames,
		UserOauthIds:         dbOrg.OauthIds,
	}, nil
}

// GetUser implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) GetUser(
	ctx context.Context,
	req *pb.GetUserRequest,
) (*pb.GetUserResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	guParams := db.GetUserByOAuthIDParams{
		OauthID: req.OauthId,
	}

	dbUser, err := querier.GetUserByOAuthID(ctx, guParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.GetUserByOAuthID(%+v)", guParams)

		return nil, status.Errorf(
			codes.NotFound,
			"User with OAuth ID '%s' not found",
			req.OauthId,
		)
	}

	metadata, err := jsonbToStruct(dbUser.Metadata)
	if err != nil {
		l.Error().Err(err).Msgf("jsonbToStruct(%s)", dbUser.Metadata)

		return nil, status.Errorf(
			codes.Internal,
			"Error parsing metadata for user with OAuth ID '%s'",
			req.OauthId,
		)
	}

	goParams := db.GetOrgByNameParams{
		OrgName: dbUser.OrgName,
	}

	dbOrg, err := querier.GetOrgByName(ctx, goParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.GetOrgByName(%+v)", goParams)

		return nil, status.Errorf(
			codes.Internal,
			"Error fetching organisation for user with OAuth ID '%s'",
			req.OauthId,
		)
	}

	return &pb.GetUserResponse{
		UserId:               dbUser.UserUuid.String(),
		OauthId:              dbUser.OauthID,
		Organisation:         dbUser.OrgName,
		LocationPolicyGroups: dbOrg.LocationPolicyGroupNames,
		CreatedAt:            &timestamppb.Timestamp{},
		Metadata:             metadata,
	}, nil
}

// UpdateLocationPolicyGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) UpdateLocationPolicyGroup(
	ctx context.Context,
	req *pb.UpdateLocationPolicyGroupRequest,
) (*pb.UpdateLocationPolicyGroupResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	// Get the location policy group as it currently is
	lpgUuid := uuid.MustParse(req.LocationPolicyGroupId)
	ggParams := db.GetLocationPolicyGroupByUUIDParams{
		LocationPolicyGroupUuid: lpgUuid,
	}

	dbLpg, err := querier.GetLocationPolicyGroupByUUID(ctx, ggParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.GetLocationPolicyGroupByUUID(%+v)", ggParams)

		return nil, status.Errorf(
			codes.NotFound,
			"Location policy group with ID '%s' not found",
			req.LocationPolicyGroupId,
		)
	}

	// Update the name, if desired
	if req.Name != "" {
		ugParams := db.UpdateLocationPolicyGroupParams{
			LocationPolicyGroupUuid: lpgUuid,
			LocationPolicyGroupName: req.Name,
		}

		dbLpg, err = querier.UpdateLocationPolicyGroup(ctx, ugParams)
		if err != nil {
			l.Error().Err(err).Msgf("querier.UpdateLocationPolicyGroup(%+v)", ugParams)

			return nil, status.Errorf(
				codes.Internal,
				"Error updating location policy group with ID '%s'",
				req.LocationPolicyGroupId,
			)
		}
	}

	// Update the policies, if desired
	if len(req.LocationPolicies) > 0 {
		// First, remove all existing policies
		daParams := db.DeleteAllLocationPoliciesFromGroupParams{
			LocationPolicyGroupUuid: lpgUuid,
		}

		err := querier.DeleteAllLocationPoliciesFromGroup(ctx, daParams)
		if err != nil {
			l.Error().Err(err).Msgf("querier.DeleteAllLocationPoliciesFromGroup(%+v)", daParams)

			return nil, status.Errorf(
				codes.Internal,
				"Error removing existing policies from location policy group with ID '%s'",
				req.LocationPolicyGroupId,
			)
		}

		// Then, add the new policies
		for _, p := range req.LocationPolicies {
			locUuid := uuid.MustParse(p.LocationId)
			apParams := db.AddLocationPolicesToGroupParams{
				RoleName:                p.Scope,
				SourceTypeName:          p.EnergySource.String(),
				LocationPolicyGroupName: dbLpg.LocationPolicyGroupName,
				LocationUuids:           []uuid.UUID{locUuid},
			}

			err = querier.AddLocationPolicesToGroup(ctx, apParams)
			if err != nil {
				l.Error().Err(err).Msgf("querier.AddLocationPolicesToGroup(%+v)", apParams)

				return nil, status.Errorf(
					codes.Internal,
					"Error adding policy for location '%s' to location policy group with ID '%s'",
					p.LocationId,
					req.LocationPolicyGroupId,
				)
			}
		}
	}

	// Fetch the policies
	llpParams := db.ListLocationPoliciesByGroupParams{
		LocationPolicyGroupUuid: lpgUuid,
	}

	dbPolicies, err := querier.ListLocationPoliciesByGroup(ctx, llpParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.ListLocationPoliciesByGroup(%+v)", llpParams)

		return nil, status.Errorf(
			codes.Internal,
			"Error fetching updated policies for location policy group with ID '%s'",
			req.LocationPolicyGroupId,
		)
	}

	policies := make([]*pb.CreateLocationPolicyGroupRequest_LocationPolicy, len(dbPolicies))
	for i, p := range dbPolicies {
		policies[i] = &pb.CreateLocationPolicyGroupRequest_LocationPolicy{
			LocationId:   p.LocationUuid.String(),
			EnergySource: pb.EnergySource(pb.EnergySource_value[p.SourceTypeName]),
			Scope:        p.RoleName,
		}
	}

	return &pb.UpdateLocationPolicyGroupResponse{
		LocationPolicyGroupId: lpgUuid.String(),
		Name:                  dbLpg.LocationPolicyGroupName,
		LocationPolicies:      policies,
	}, nil
}

// UpdateOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) UpdateOrganisation(
	ctx context.Context,
	req *pb.UpdateOrganisationRequest,
) (*pb.UpdateOrganisationResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))
	// Get the org as it currently is
	goParams := db.GetOrgByNameParams{
		OrgName: req.OrgName,
	}

	dbOrg, err := querier.GetOrgByName(ctx, goParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.GetOrgByName(%+v)", goParams)

		return nil, status.Errorf(
			codes.NotFound,
			"Organisation with name '%s' not found",
			req.OrgName,
		)
	}

	// Set the metadata to update, if desired; else keep as is
	metadata := dbOrg.Metadata
	if req.Metadata != nil {
		metadata, err = req.Metadata.MarshalJSON()
		if err != nil {
			l.Err(err).Msgf("req.Metadata.MarshalJSON()")

			return nil, status.Error(
				codes.InvalidArgument,
				"Invalid metadata. Ensure object is JSON serializable.",
			)
		}
	}

	// Set the name to update, if desired; else keep as is
	name := dbOrg.OrgName
	if req.NewName != "" {
		name = req.NewName
	}

	// Update the location policy groups, if desired; else keep as is
	if len(req.LocationPolicyGroupIds) > 0 {
		// Remove the existing groups
		rlpgParams := db.RemoveLocationPolicyGroupsFromOrgParams{
			OrgUuid:                  dbOrg.OrgUuid,
			LocationPolicyGroupUuids: dbOrg.LocationPolicyGroupUuids,
		}

		err := querier.RemoveLocationPolicyGroupsFromOrg(ctx, rlpgParams)
		if err != nil {
			l.Error().Err(err).Msgf("querier.RemoveLocationPolicyGroupsFromOrg(%+v)", rlpgParams)

			return nil, status.Errorf(
				codes.Internal,
				"Error removing existing location policy groups from organisation with ID '%s'",
				dbOrg.OrgUuid,
			)
		}

		// Add the new groups
		lpgUuids := make([]uuid.UUID, len(req.LocationPolicyGroupIds))
		for i, id := range req.LocationPolicyGroupIds {
			lpgUuids[i] = uuid.MustParse(id)
		}
		alpgParams := db.AddLocationPolicyGroupsToOrgParams{
			OrgUuid:                  dbOrg.OrgUuid,
			LocationPolicyGroupUuids: lpgUuids,
		}

		err = querier.AddLocationPolicyGroupsToOrg(ctx, alpgParams)
		if err != nil {
			l.Error().Err(err).Msgf("querier.AddLocationPolicyGroupsToOrg(%+v)", alpgParams)

			return nil, status.Errorf(
				codes.Internal,
				"Error adding location policy groups to organisation '%s'. "+
					"Ensure all location policy group IDs exist.",
				req.OrgName,
			)
		}
	}

	uoParams := db.UpdateOrgParams{
		OrgUuid:  dbOrg.OrgUuid,
		OrgName:  name,
		Metadata: metadata,
	}

	_, err = querier.UpdateOrg(ctx, uoParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.UpdateOrg(%+v)", uoParams)

		return nil, status.Errorf(
			codes.Internal,
			"Error updating organisation '%s'. Ensure name is unique and metadata is valid JSON.",
			req.OrgName,
		)
	}

	// Get updated org
	goParams = db.GetOrgByNameParams{
		OrgName: name,
	}

	dbOrg, err = querier.GetOrgByName(ctx, goParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.GetOrgByName(%+v)", goParams)

		return nil, status.Errorf(
			codes.Internal,
			"Error fetching updated organisation '%s'",
			req.OrgName,
		)
	}

	metadata2, err := jsonbToStruct(dbOrg.Metadata)
	if err != nil {
		l.Error().Err(err).Msgf("jsonbToStruct(%s)", dbOrg.Metadata)

		return nil, status.Errorf(
			codes.Internal,
			"Error parsing metadata for organisation '%s'",
			req.OrgName,
		)
	}

	return &pb.UpdateOrganisationResponse{
		OrgId:                dbOrg.OrgUuid.String(),
		OrgName:              dbOrg.OrgName,
		Metadata:             metadata2,
		CreatedAt:            timestamppb.New(dbOrg.CreatedAtUtc.Time),
		LocationPolicyGroups: dbOrg.LocationPolicyGroupNames,
		UserOauthIds:         dbOrg.OauthIds,
	}, nil
}

// Compile-time check to ensure the interface is implemented fully.
var _ pb.DataPlatformAdministrationServiceServer = (*DataPlatformAdministrationServiceServerImpl)(
	nil,
)

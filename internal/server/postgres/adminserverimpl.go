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
// It also expects a zerolog logger to be set in the context.
type DataPlatformAdministrationServiceServerImpl struct{}

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

	gprms := db.GetLocationPolicyGroupByNameParams{
		LocationPolicyGroupName: req.LocationPolicyGroupName,
	}

	dbGroup, err := querier.GetLocationPolicyGroupByName(ctx, gprms)
	if err != nil {
		l.Error().Err(err).Msgf("querier.GetLocationPolicyGroupByName(%+v)", gprms)

		return nil, status.Errorf(
			codes.NotFound,
			"No location policy group found with name '%s'",
			req.LocationPolicyGroupName,
		)
	}

	llpParams := db.ListLocationPoliciesByGroupParams{
		LocationPolicyGroupUuid: dbGroup.LocationPolicyGroupUuid,
	}

	dbPolicies, err := querier.ListLocationPoliciesByGroup(ctx, llpParams)
	if err != nil {
		l.Error().Err(err).Msgf("querier.ListLocationPoliciesByGroup(%+v)", llpParams)

		return nil, status.Errorf(
			codes.NotFound,
			"No location policy group found with ID '%s'",
			dbGroup.LocationPolicyGroupUuid,
		)
	}

	policies := make([]*pb.LocationPolicy, len(dbPolicies))
	for i, p := range dbPolicies {
		policies[i] = &pb.LocationPolicy{
			LocationId:   p.LocationUuid.String(),
			EnergySource: pb.EnergySource(p.SourceTypeID),
			Permission:   pb.Permission(p.PermissionID),
		}
	}

	return &pb.GetLocationPolicyGroupResponse{
		LocationPolicyGroupId: dbGroup.LocationPolicyGroupUuid.String(),
		Name:                  dbGroup.LocationPolicyGroupName,
		LocationPolicies:      policies,
	}, nil
}

// AddLocationPoliciesToGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) AddLocationPoliciesToGroup(
	ctx context.Context,
	req *pb.AddLocationPoliciesToGroupRequest,
) (*pb.AddLocationPoliciesToGroupResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	for _, p := range req.LocationPolicies {
		locUuid := uuid.MustParse(p.LocationId)
		apParams := db.AddLocationPolicesToGroupParams{
			PermissionID:            int16(p.Permission),
			SourceTypeID:            int16(p.EnergySource),
			LocationPolicyGroupName: req.LocationPolicyGroupName,
			LocationUuids:           []uuid.UUID{locUuid},
		}

		err := querier.AddLocationPolicesToGroup(ctx, apParams)
		if err != nil {
			l.Error().Err(err).Msgf("querier.AddLocationPolicesToGroup(%+v)", apParams)

			return nil, status.Errorf(
				codes.Internal,
				"No location policy group found with name '%s'",
				req.LocationPolicyGroupName,
			)
		}
	}

	return &pb.AddLocationPoliciesToGroupResponse{}, nil
}

// RemoveLocationPoliciesFromGroup implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) RemoveLocationPoliciesFromGroup(
	ctx context.Context,
	req *pb.RemoveLocationPoliciesFromGroupRequest,
) (*pb.RemoveLocationPoliciesFromGroupResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	for _, p := range req.LocationPolicies {
		locUuid := uuid.MustParse(p.LocationId)
		rpParams := db.RemoveLocationPoliciesFromGroupParams{
			PermissionID:            int16(p.Permission),
			SourceTypeID:            int16(p.EnergySource),
			LocationPolicyGroupName: req.LocationPolicyGroupName,
			LocationUuid:            locUuid,
		}

		err := querier.RemoveLocationPoliciesFromGroup(ctx, rpParams)
		if err != nil {
			l.Error().Err(err).Msgf("querier.RemoveLocationPoliciesFromGroup(%+v)", rpParams)

			return nil, status.Errorf(
				codes.Internal,
				"No location policy group found with name '%s'",
				req.LocationPolicyGroupName,
			)
		}
	}

	return &pb.RemoveLocationPoliciesFromGroupResponse{}, nil
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

// AddLocationPolicyGroupToOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) AddLocationPolicyGroupToOrganisation(
	ctx context.Context,
	req *pb.AddLocationPolicyGroupToOrganisationRequest,
) (*pb.AddLocationPolicyGroupToOrganisationResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	agprms := db.AddLocationPolicyGroupToOrgByNamesParams{
		OrgName:                 req.OrgName,
		LocationPolicyGroupName: req.LocationPolicyGroupName,
	}

	err := querier.AddLocationPolicyGroupToOrgByNames(ctx, agprms)
	if err != nil {
		l.Error().Err(err).Msgf("querier.AddLocationPolicyGroupToOrgByNames(%+v)", agprms)

		return nil, status.Errorf(
			codes.Internal,
			"Error adding location policy group '%s' to organisation '%s'. "+
				"Ensure organisation and location policy group exist.",
			req.LocationPolicyGroupName,
			req.OrgName,
		)
	}

	return &pb.AddLocationPolicyGroupToOrganisationResponse{}, nil
}

// RemoveLocationPolicyGroupFromOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) RemoveLocationPolicyGroupFromOrganisation(
	ctx context.Context,
	req *pb.RemoveLocationPolicyGroupFromOrganisationRequest,
) (*pb.RemoveLocationPolicyGroupFromOrganisationResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	rgprms := db.RemoveLocationPolicyGroupFromOrgByNamesParams{
		OrgName:                 req.OrgName,
		LocationPolicyGroupName: req.LocationPolicyGroupName,
	}

	err := querier.RemoveLocationPolicyGroupFromOrgByNames(ctx, rgprms)
	if err != nil {
		l.Error().Err(err).Msgf("querier.RemoveLocationPolicyGroupFromOrgByNames(%+v)", rgprms)

		return nil, status.Errorf(
			codes.Internal,
			"Error removing location policy group '%s' from organisation '%s'. "+
				"Ensure organisation and location policy group exist.",
			req.LocationPolicyGroupName,
			req.OrgName,
		)
	}

	return &pb.RemoveLocationPolicyGroupFromOrganisationResponse{}, nil
}

// AddUserToOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) AddUserToOrganisation(
	ctx context.Context,
	req *pb.AddUserToOrganisationRequest,
) (*pb.AddUserToOrganisationResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	auprms := db.AddUserToOrgByOAuthIDAndNameParams{
		OrgName: req.OrgName,
		OauthID: req.UserOauthId,
	}

	err := querier.AddUserToOrgByOAuthIDAndName(ctx, auprms)
	if err != nil {
		l.Error().Err(err).Msgf("querier.AddUserToOrgByOAuthIDAndName(%+v)", auprms)

		return nil, status.Errorf(
			codes.Internal,
			"Error adding user with OAuth ID '%s' to organisation '%s'. "+
				"Ensure organisation and user exist.",
			req.UserOauthId,
			req.OrgName,
		)
	}

	return &pb.AddUserToOrganisationResponse{}, nil
}

// RemoveUserFromOrganisation implements dp.DataPlatformAdministrationServiceServer.
func (d *DataPlatformAdministrationServiceServerImpl) RemoveUserFromOrganisation(
	ctx context.Context,
	req *pb.RemoveUserFromOrganisationRequest,
) (*pb.RemoveUserFromOrganisationResponse, error) {
	l := zerolog.Ctx(ctx)
	querier := db.New(ix.GetTxFromContext(ctx))

	ruprms := db.RemoveUserFromOrgByOAuthIDAndNameParams{
		OrgName: req.OrgName,
		OauthID: req.UserOauthId,
	}

	err := querier.RemoveUserFromOrgByOAuthIDAndName(ctx, ruprms)
	if err != nil {
		l.Error().Err(err).Msgf("querier.RemoveUserFromOrgByOAuthIDAndName(%+v)", ruprms)

		return nil, status.Errorf(
			codes.Internal,
			"Error removing user with OAuth ID '%s' from organisation '%s'. "+
				"Ensure organisation and user exist.",
			req.UserOauthId,
			req.OrgName,
		)
	}

	return &pb.RemoveUserFromOrganisationResponse{}, nil
}

// Compile-time check to ensure the interface is implemented fully.
var _ pb.DataPlatformAdministrationServiceServer = (*DataPlatformAdministrationServiceServerImpl)(
	nil,
)

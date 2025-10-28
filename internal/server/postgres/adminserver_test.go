package postgres

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pb "github.com/openclimatefix/data-platform/internal/gen/ocf/dp"
)

func TestCreateOrganisation(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	testCases := []struct {
		name      string
		createReq *pb.CreateOrganisationRequest
	}{
		{
			name: "Should create organisation",
			createReq: &pb.CreateOrganisationRequest{
				OrgName:  "test_create_organisation_1",
				Metadata: metadata,
			},
		},
		{
			name: "Shouldn't create organisation with duplicate name",
			createReq: &pb.CreateOrganisationRequest{
				OrgName:  "test_create_organisation_1",
				Metadata: metadata,
			},
		},
		{
			name: "Shouldn't create organisation with empty name",
			createReq: &pb.CreateOrganisationRequest{
				OrgName:  "",
				Metadata: metadata,
			},
		},
		{
			name: "Should create another organisation",
			createReq: &pb.CreateOrganisationRequest{
				OrgName:  "test_create_organisation_2",
				Metadata: metadata,
			},
		},
		{
			name: "Should create organisation with empty metadata",
			createReq: &pb.CreateOrganisationRequest{
				OrgName:  "test_create_organisation_3",
				Metadata: nil,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := ac.CreateOrganisation(context.Background(), tc.createReq)
			if strings.Split(tc.name, " ")[0] == "Shouldn't" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				// Read back the organisation
				dbOrg, err := ac.GetOrganisation(context.Background(), &pb.GetOrganisationRequest{
					OrgName: resp.OrgName,
				})
				require.NoError(t, err)

				require.Equal(t, tc.createReq.OrgName, dbOrg.OrgName)
				require.Equal(t, tc.createReq.Metadata.AsMap(), dbOrg.Metadata.AsMap())
			}
		})
	}
}

func TestUpdateOrganisation(t *testing.T) {
	metadata1, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)
	metadata2, err := structpb.NewStruct(map[string]any{"source": "updated_test"})
	require.NoError(t, err)

	createResp, err := ac.CreateOrganisation(context.Background(), &pb.CreateOrganisationRequest{
		OrgName:  "test_update_organisation",
		Metadata: metadata1,
	})
	require.NoError(t, err)

	testCases := []struct {
		name             string
		updateReq        *pb.UpdateOrganisationRequest
		expectedName     string
		expectedMetadata *structpb.Struct
	}{
		{
			name: "Should update organisation name and metadata",
			updateReq: &pb.UpdateOrganisationRequest{
				OrgName:  createResp.OrgName,
				NewName:  "test_update_organisation_updated",
				Metadata: metadata2,
			},
			expectedName:     "test_update_organisation_updated",
			expectedMetadata: metadata2,
		},
		{
			name: "Should update only organisation name if metadata is nil",
			updateReq: &pb.UpdateOrganisationRequest{
				OrgName:  "test_update_organisation_updated",
				NewName:  "test_update_organisation_name_only",
				Metadata: nil,
			},
			expectedName:     "test_update_organisation_name_only",
			expectedMetadata: metadata2,
		},
		{
			name: "Should update only metadata if name is empty",
			updateReq: &pb.UpdateOrganisationRequest{
				OrgName:  "test_update_organisation_name_only",
				Metadata: metadata1,
			},
			expectedName:     "test_update_organisation_name_only",
			expectedMetadata: metadata1,
		},
		{
			name: "Shouldn't update non-existent organisation",
			updateReq: &pb.UpdateOrganisationRequest{
				OrgName:  "non_existent_org_id",
				NewName:  "should_not_update",
				Metadata: metadata1,
			},
		},
		{
			name: "Should do nothing if both name and metadata are empty",
			updateReq: &pb.UpdateOrganisationRequest{
				OrgName: "test_update_organisation_name_only",
			},
			expectedName:     "test_update_organisation_name_only",
			expectedMetadata: metadata1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ac.UpdateOrganisation(context.Background(), tc.updateReq)
			if strings.Split(tc.name, " ")[0] == "Shouldn't" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				// Read back the organisation
				dbOrg, err := ac.GetOrganisation(context.Background(), &pb.GetOrganisationRequest{
					OrgName: tc.expectedName,
				})
				require.NoError(t, err)

				require.Equal(t, tc.expectedName, dbOrg.OrgName)
				require.Equal(t, tc.expectedMetadata.AsMap(), dbOrg.Metadata.AsMap())
			}
		})
	}
}

func TestDeleteOrganisation(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	createResp, err := ac.CreateOrganisation(context.Background(), &pb.CreateOrganisationRequest{
		OrgName:  "test_delete_organisation",
		Metadata: metadata,
	})
	require.NoError(t, err)

	testCases := []struct {
		name      string
		deleteReq *pb.DeleteOrganisationRequest
	}{
		{
			name: "Should delete existing organisation",
			deleteReq: &pb.DeleteOrganisationRequest{
				OrgId: createResp.OrgId,
			},
		},
		{
			name: "Shouldn't delete non-existent organisation",
			deleteReq: &pb.DeleteOrganisationRequest{
				OrgId: "non_existent_org_id",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ac.DeleteOrganisation(context.Background(), tc.deleteReq)
			if strings.Split(tc.name, " ")[0] == "Shouldn't" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				// Try to read back the organisation
				_, err := ac.GetOrganisation(context.Background(), &pb.GetOrganisationRequest{
					OrgName: "test_delete_organisation",
				})
				// Obviously should error here
				require.Error(t, err)
			}
		})
	}
}

func TestCreateUser(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	orgResp, err := ac.CreateOrganisation(context.Background(), &pb.CreateOrganisationRequest{
		OrgName:  "test_create_user_organisation",
		Metadata: metadata,
	})
	require.NoError(t, err)

	testCases := []struct {
		name      string
		createReq *pb.CreateUserRequest
	}{
		{
			name: "Should create user",
			createReq: &pb.CreateUserRequest{
				OauthId:      "TEST_CREATE_USER_1",
				Organisation: orgResp.OrgName,
				Metadata:     metadata,
			},
		},
		{
			name: "Shouldn't create user with duplicate oauth ID",
			createReq: &pb.CreateUserRequest{
				OauthId:      "TEST_CREATE_USER_1",
				Organisation: orgResp.OrgName,
				Metadata:     metadata,
			},
		},
		{
			name: "Shouldn't create user with empty oauth ID",
			createReq: &pb.CreateUserRequest{
				OauthId:      "",
				Organisation: orgResp.OrgName,
				Metadata:     metadata,
			},
		},
		{
			name: "Should create another user",
			createReq: &pb.CreateUserRequest{
				OauthId:      "TEST_CREATE_USER_2",
				Organisation: orgResp.OrgName,
				Metadata:     metadata,
			},
		},
		{
			name: "Should create user with empty metadata",
			createReq: &pb.CreateUserRequest{
				OauthId:      "TEST_CREATE_USER_3",
				Organisation: orgResp.OrgName,
				Metadata:     nil,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ac.CreateUser(context.Background(), tc.createReq)
			if strings.Split(tc.name, " ")[0] == "Shouldn't" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				// Read back the user
				dbUser, err := ac.GetUser(context.Background(), &pb.GetUserRequest{
					OauthId: tc.createReq.OauthId,
				})
				require.NoError(t, err)

				require.Equal(t, tc.createReq.OauthId, dbUser.OauthId)
				require.Equal(t, tc.createReq.Organisation, dbUser.Organisation)
				require.Equal(t, tc.createReq.Metadata.AsMap(), dbUser.Metadata.AsMap())
			}
		})
	}
}

func DeleteUser(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	orgResp, err := ac.CreateOrganisation(context.Background(), &pb.CreateOrganisationRequest{
		OrgName:  "test_delete_user_organisation",
		Metadata: metadata,
	})
	require.NoError(t, err)

	createResp, err := ac.CreateUser(context.Background(), &pb.CreateUserRequest{
		OauthId:      "TEST_DELETE_USER",
		Organisation: orgResp.OrgName,
		Metadata:     metadata,
	})
	require.NoError(t, err)

	testCases := []struct {
		name      string
		deleteReq *pb.DeleteUserRequest
	}{
		{
			name: "Should delete existing user",
			deleteReq: &pb.DeleteUserRequest{
				UserId: createResp.UserId,
			},
		},
		{
			name: "Shouldn't delete non-existent user",
			deleteReq: &pb.DeleteUserRequest{
				UserId: "non_existent_user_id",
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ac.DeleteUser(context.Background(), tc.deleteReq)
			if strings.Split(tc.name, " ")[0] == "Shouldn't" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				// Try to read back the user
				_, err := ac.GetUser(context.Background(), &pb.GetUserRequest{
					OauthId: "TEST_DELETE_USER",
				})
				// Obviously should error here
				require.Error(t, err)
			}
		})
	}
}

func TestListUserLocations(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	orgResp, err := ac.CreateOrganisation(context.Background(), &pb.CreateOrganisationRequest{
		OrgName:  "test_list_user_locations_organisation",
		Metadata: metadata,
	})
	require.NoError(t, err)

	pgResp, err := ac.CreateLocationPolicyGroup(
		context.Background(),
		&pb.CreateLocationPolicyGroupRequest{
			Name: "test_list_user_locations_policy_group",
		},
	)

	require.NoError(t, err)
	_, err = ac.UpdateOrganisation(context.Background(), &pb.UpdateOrganisationRequest{
		OrgName:                orgResp.OrgName,
		LocationPolicyGroupIds: []string{pgResp.LocationPolicyGroupId},
	})
	require.NoError(t, err)

	_, err = ac.CreateUser(context.Background(), &pb.CreateUserRequest{
		OauthId:      "TEST_LIST_USER_LOCATIONS_USER001",
		Organisation: orgResp.OrgName,
		Metadata:     metadata,
	})
	require.NoError(t, err)

	locationNames := []string{
		"test_list_user_locations_location_a",
		"test_list_user_locations_location_b",
	}
	for _, locName := range locationNames {
		locResp, err := dc.CreateLocation(context.Background(), &pb.CreateLocationRequest{
			LocationName:           locName,
			Metadata:               metadata,
			EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
			GeometryWkt:            "POINT(10 10)",
			EffectiveCapacityWatts: 1000,
			LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
			ValidFromUtc:           timestamppb.New(time.Now().UTC().Add(-time.Hour)),
		})
		require.NoError(t, err)
		_, err = ac.AddLocationPoliciesToGroup(
			context.Background(),
			&pb.AddLocationPoliciesToGroupRequest{
				LocationPolicies: []*pb.LocationPolicy{
					{
						LocationId:   locResp.LocationUuid,
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						Permission:   pb.Permission_PERMISSION_WRITE,
					},
				},
				LocationPolicyGroupName: pgResp.Name,
			},
		)
		require.NoError(t, err)
	}

	listResp, err := ac.ListUserLocations(context.Background(), &pb.ListUserLocationsRequest{
		OauthId: "TEST_LIST_USER_LOCATIONS_USER001",
	})
	require.NoError(t, err)
	require.Equal(t, len(locationNames), len(listResp.Locations))

	returnedLocationNames := make(map[string]bool)
	for _, loc := range listResp.Locations {
		returnedLocationNames[loc.LocationName] = true
	}

	for _, locName := range locationNames {
		require.True(t, returnedLocationNames[locName])
	}
}

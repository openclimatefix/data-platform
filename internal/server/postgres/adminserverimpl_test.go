package postgres

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
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
			resp, err := ac.CreateOrganisation(t.Context(), tc.createReq)
			if strings.Split(tc.name, " ")[0] == "Shouldn't" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				// Read back the organisation
				dbOrg, err := ac.GetOrganisation(t.Context(), &pb.GetOrganisationRequest{
					OrgName: resp.OrgName,
				})
				require.NoError(t, err)

				require.Equal(t, tc.createReq.OrgName, dbOrg.OrgName)
				require.Equal(t, tc.createReq.Metadata.AsMap(), dbOrg.Metadata.AsMap())
			}
		})
	}
}

func TestDeleteOrganisation(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	orgResp, err := ac.CreateOrganisation(t.Context(), &pb.CreateOrganisationRequest{
		OrgName:  "test_delete_organisation_1",
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
				OrgName: orgResp.OrgName,
			},
		},
		{
			name: "Should handle deleting a non-existent organisation",
			deleteReq: &pb.DeleteOrganisationRequest{
				OrgName: "non_existent_delete_organisation",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ac.DeleteOrganisation(t.Context(), tc.deleteReq)
			if strings.Contains(tc.name, "Shouldn't") {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				_, err := ac.GetOrganisation(t.Context(), &pb.GetOrganisationRequest{
					OrgName: tc.deleteReq.OrgName,
				})
				require.Error(t, err)
			}
		})
	}
}

func TestAddRemoveLocationPolicyGroupToOrganisation(t *testing.T) {
	orgResp, err := ac.CreateOrganisation(t.Context(), &pb.CreateOrganisationRequest{
		OrgName: "test_add_remove_location_policy_group_organisation",
	})
	require.NoError(t, err)
	lpResp, err := ac.CreateLocationPolicyGroup(t.Context(), &pb.CreateLocationPolicyGroupRequest{
		Name: "test_add_remove_location_policy_group_policy_group",
	})
	require.NoError(t, err)

	testCases := []struct {
		name                     string
		addRequest               *pb.AddLocationPolicyGroupToOrganisationRequest
		removeRequest            *pb.RemoveLocationPolicyGroupFromOrganisationRequest
		expectedPolicyGroupCount int
	}{
		{
			name: "Should add location policy group to organisation",
			addRequest: &pb.AddLocationPolicyGroupToOrganisationRequest{
				OrgName:                 orgResp.OrgName,
				LocationPolicyGroupName: lpResp.Name,
			},
			expectedPolicyGroupCount: 1,
		},
		{
			name: "Should handle adding duplicate location policy groups",
			addRequest: &pb.AddLocationPolicyGroupToOrganisationRequest{
				OrgName:                 orgResp.OrgName,
				LocationPolicyGroupName: lpResp.Name,
			},
			expectedPolicyGroupCount: 1,
		},
		{
			name: "Should remove location policy group from organisation",
			removeRequest: &pb.RemoveLocationPolicyGroupFromOrganisationRequest{
				OrgName:                 orgResp.OrgName,
				LocationPolicyGroupName: lpResp.Name,
			},
			expectedPolicyGroupCount: 0,
		},
		{
			name: "Should handle non-existent location policy group removal",
			removeRequest: &pb.RemoveLocationPolicyGroupFromOrganisationRequest{
				OrgName:                 orgResp.OrgName,
				LocationPolicyGroupName: "non_existent_policy_group",
			},
			expectedPolicyGroupCount: 0,
		},
		{
			name: "Shouldn't add non-existent location policy group to organisation",
			addRequest: &pb.AddLocationPolicyGroupToOrganisationRequest{
				OrgName:                 orgResp.OrgName,
				LocationPolicyGroupName: "non_existent_policy_group",
			},
			expectedPolicyGroupCount: 0,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.addRequest != nil {
				_, err = ac.AddLocationPolicyGroupToOrganisation(t.Context(), tc.addRequest)
			}

			if tc.removeRequest != nil {
				_, err = ac.RemoveLocationPolicyGroupFromOrganisation(t.Context(), tc.removeRequest)
			}

			if strings.Contains(tc.name, "Shouldn't") {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Read back the organisation
				dbOrg, err := ac.GetOrganisation(t.Context(), &pb.GetOrganisationRequest{
					OrgName: orgResp.OrgName,
				})
				require.NoError(t, err)
				require.Equal(t, tc.expectedPolicyGroupCount, len(dbOrg.LocationPolicyGroups))
			}
		})
	}
}

func TestAddRemoveLocationPoliciesFromGroup(t *testing.T) {
	lResp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
		LocationName:           "test_add_remove_location_policies_location",
		EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
		GeometryWkt:            "POINT(8.3 33.44)",
		EffectiveCapacityWatts: 5000,
		LocationType:           pb.LocationType_LOCATION_TYPE_SITE,
		ValidFromUtc:           timestamppb.New(time.Now().UTC().Add(-1 * time.Hour)),
	})
	require.NoError(t, err)
	lpResp, err := ac.CreateLocationPolicyGroup(t.Context(), &pb.CreateLocationPolicyGroupRequest{
		Name: "test_add_remove_location_policies_policy_group",
	})
	require.NoError(t, err)

	testCases := []struct {
		name                string
		addRequest          *pb.AddLocationPoliciesToGroupRequest
		removeRequest       *pb.RemoveLocationPoliciesFromGroupRequest
		expectedPolicyCount int
	}{
		{
			name: "Should add location policies to group",
			addRequest: &pb.AddLocationPoliciesToGroupRequest{
				LocationPolicyGroupName: lpResp.Name,
				LocationPolicies: []*pb.LocationPolicy{
					{
						LocationId:   lResp.LocationUuid,
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						Permission:   pb.Permission_PERMISSION_READ,
					},
				},
			},
			expectedPolicyCount: 1,
		},
		{
			name: "Should handle adding duplicate location policies",
			addRequest: &pb.AddLocationPoliciesToGroupRequest{
				LocationPolicyGroupName: lpResp.Name,
				LocationPolicies: []*pb.LocationPolicy{
					{
						LocationId:   lResp.LocationUuid,
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						Permission:   pb.Permission_PERMISSION_READ,
					},
				},
			},
			expectedPolicyCount: 1,
		},
		// {
		// 	name: "Shouldn't add location policy referencing non-existent source",
		// 	addRequest: &pb.AddLocationPoliciesToGroupRequest{
		//		LocationPolicyGroupName: lpResp.Name,
		//		LocationPolicies:        []*pb.LocationPolicy{
		//			{
		//				LocationId:   lResp.LocationUuid,
		//				EnergySource: pb.EnergySource_ENERGY_SOURCE_WIND,
		//				Permission:   pb.Permission_PERMISSION_READ,
		//			},
		//		},
		//	},
		//	expectedPolicyCount: 1,
		// },
		{
			name: "Shouldn't add location policy referencing non-existent location",
			addRequest: &pb.AddLocationPoliciesToGroupRequest{
				LocationPolicyGroupName: lpResp.Name,
				LocationPolicies: []*pb.LocationPolicy{
					{
						LocationId:   uuid.New().String(),
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						Permission:   pb.Permission_PERMISSION_READ,
					},
				},
			},
			expectedPolicyCount: 1,
		},
		{
			name: "Should remove location policies from group",
			removeRequest: &pb.RemoveLocationPoliciesFromGroupRequest{
				LocationPolicyGroupName: lpResp.Name,
				LocationPolicies: []*pb.LocationPolicy{
					{
						LocationId:   lResp.LocationUuid,
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						Permission:   pb.Permission_PERMISSION_READ,
					},
				},
			},
			expectedPolicyCount: 0,
		},
		{
			name: "Should handle non-existent location policy removal",
			removeRequest: &pb.RemoveLocationPoliciesFromGroupRequest{
				LocationPolicyGroupName: lpResp.Name,
				LocationPolicies: []*pb.LocationPolicy{
					{
						LocationId:   uuid.New().String(),
						EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
						Permission:   pb.Permission_PERMISSION_READ,
					},
				},
			},
			expectedPolicyCount: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.addRequest != nil {
				_, err = ac.AddLocationPoliciesToGroup(t.Context(), tc.addRequest)
			}

			if tc.removeRequest != nil {
				_, err = ac.RemoveLocationPoliciesFromGroup(t.Context(), tc.removeRequest)
			}

			if strings.Contains(tc.name, "Shouldn't") {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				// Read back the location policy group
				dbLPG, err := ac.GetLocationPolicyGroup(
					t.Context(),
					&pb.GetLocationPolicyGroupRequest{
						LocationPolicyGroupName: lpResp.Name,
					},
				)
				require.NoError(t, err)
				require.Equal(t, tc.expectedPolicyCount, len(dbLPG.LocationPolicies))
			}
		})
	}
}

func TestCreateUser(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	orgResp, err := ac.CreateOrganisation(t.Context(), &pb.CreateOrganisationRequest{
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
			_, err := ac.CreateUser(t.Context(), tc.createReq)
			if strings.Split(tc.name, " ")[0] == "Shouldn't" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				// Read back the user
				dbUser, err := ac.GetUser(t.Context(), &pb.GetUserRequest{
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

	orgResp, err := ac.CreateOrganisation(t.Context(), &pb.CreateOrganisationRequest{
		OrgName:  "test_delete_user_organisation",
		Metadata: metadata,
	})
	require.NoError(t, err)

	createResp, err := ac.CreateUser(t.Context(), &pb.CreateUserRequest{
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
			_, err := ac.DeleteUser(t.Context(), tc.deleteReq)
			if strings.Split(tc.name, " ")[0] == "Shouldn't" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)

				// Try to read back the user
				_, err := ac.GetUser(t.Context(), &pb.GetUserRequest{
					OauthId: "TEST_DELETE_USER",
				})
				// Obviously should error here
				require.Error(t, err)
			}
		})
	}
}

func TestListLocationsIamFilters(t *testing.T) {
	pivotTime := time.Now().Truncate(time.Minute)

	// Create a bunch of locations
	var (
		locationUuids    []string
		locationPolicies []*pb.LocationPolicy
	)

	for i := range 5 {
		resp, err := dc.CreateLocation(t.Context(), &pb.CreateLocationRequest{
			LocationName: fmt.Sprintf(
				"test_list_locations_site_%02d",
				i,
			),
			GeometryWkt:            fmt.Sprintf("POINT(-5.%d 51.%d)", i, i),
			EffectiveCapacityWatts: uint64(1000000 + i*100),
			EnergySource:           pb.EnergySource_ENERGY_SOURCE_SOLAR,
			LocationType:           pb.LocationType_LOCATION_TYPE_GSP,
			ValidFromUtc:           timestamppb.New(pivotTime.Add(-time.Hour * 4)),
		})
		require.NoError(t, err)

		locationUuids = append(locationUuids, resp.LocationUuid)
		locationPolicies = append(locationPolicies, &pb.LocationPolicy{
			LocationId:   resp.LocationUuid,
			EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
			Permission:   pb.Permission_PERMISSION_WRITE,
		})
	}

	// Create an organisation with a user and location policies that have write access on the locations
	orgResp, err := ac.CreateOrganisation(t.Context(), &pb.CreateOrganisationRequest{
		OrgName: "test_list_locations_organisation",
	})
	require.NoError(t, err)
	user1Resp, err := ac.CreateUser(t.Context(), &pb.CreateUserRequest{
		OauthId:      "TEST_LIST_LOCATIONS_USER",
		Organisation: orgResp.OrgName,
	})
	require.NoError(t, err)
	lpResp, err := ac.CreateLocationPolicyGroup(t.Context(), &pb.CreateLocationPolicyGroupRequest{
		Name: "test_list_locations_policy_group_1",
	})
	require.NoError(t, err)
	_, err = ac.AddLocationPolicyGroupToOrganisation(
		t.Context(),
		&pb.AddLocationPolicyGroupToOrganisationRequest{
			OrgName:                 orgResp.OrgName,
			LocationPolicyGroupName: lpResp.Name,
		},
	)
	require.NoError(t, err)
	_, err = ac.AddLocationPoliciesToGroup(t.Context(), &pb.AddLocationPoliciesToGroupRequest{
		LocationPolicyGroupName: lpResp.Name,
		LocationPolicies:        locationPolicies,
	})
	require.NoError(t, err)
	// Create an organisation with a user and one read policy
	orgResp, err = ac.CreateOrganisation(t.Context(), &pb.CreateOrganisationRequest{
		OrgName: "test_list_locations_organisation_2",
	})
	require.NoError(t, err)
	user2Resp, err := ac.CreateUser(t.Context(), &pb.CreateUserRequest{
		OauthId:      "TEST_LIST_LOCATIONS_USER_2",
		Organisation: orgResp.OrgName,
	})
	require.NoError(t, err)
	lpResp, err = ac.CreateLocationPolicyGroup(t.Context(), &pb.CreateLocationPolicyGroupRequest{
		Name: "test_list_locations_policy_group_2",
	})
	require.NoError(t, err)
	_, err = ac.AddLocationPolicyGroupToOrganisation(
		t.Context(),
		&pb.AddLocationPolicyGroupToOrganisationRequest{
			OrgName:                 orgResp.OrgName,
			LocationPolicyGroupName: lpResp.Name,
		},
	)
	require.NoError(t, err)
	_, err = ac.AddLocationPoliciesToGroup(t.Context(), &pb.AddLocationPoliciesToGroupRequest{
		LocationPolicyGroupName: lpResp.Name,
		LocationPolicies: []*pb.LocationPolicy{
			{
				LocationId:   locationUuids[0],
				EnergySource: pb.EnergySource_ENERGY_SOURCE_SOLAR,
				Permission:   pb.Permission_PERMISSION_READ,
			},
		},
	})
	require.NoError(t, err)

	permissionFilter := new(pb.Permission)
	*permissionFilter = pb.Permission_PERMISSION_READ

	// All tests in the table need to filter by the location uuids just created as the postgres
	// container the tests are run against is reused across the tests for speed of unit testing.
	// As such, it may contain more than just the locations created here, depending on the number of
	// tests being run.
	// TODO: This is a fairly minimal test suite, and I imagine there are plenty of edge cases that
	// are not covered here. This purely covers the basic filtering functionality, and should by
	// improved upon in future.
	tests := []struct {
		name          string
		req           *pb.ListLocationsRequest
		expectedCount int
	}{
		{
			name: "Should filter locations by user 1",
			req: &pb.ListLocationsRequest{
				UserOauthIdFilter:   &user1Resp.OauthId,
				LocationUuidsFilter: locationUuids,
			},
			expectedCount: 5,
		},
		{
			name: "Should filter locations by user 2",
			req: &pb.ListLocationsRequest{
				UserOauthIdFilter:   &user2Resp.OauthId,
				LocationUuidsFilter: locationUuids,
			},
			expectedCount: 1,
		},
		{
			name: "Should filter locations by permission",
			req: &pb.ListLocationsRequest{
				PermissionFilter:    permissionFilter,
				LocationUuidsFilter: locationUuids,
			},
			expectedCount: 1,
		},
		{
			name: "Should filter locations by user and permission",
			req: &pb.ListLocationsRequest{
				UserOauthIdFilter:   &user2Resp.OauthId,
				PermissionFilter:    permissionFilter,
				LocationUuidsFilter: locationUuids,
			},
			expectedCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := dc.ListLocations(t.Context(), tt.req)
			if strings.Contains(tt.name, "Shouldn't") {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedCount, len(resp.Locations))
			}
		})
	}
}

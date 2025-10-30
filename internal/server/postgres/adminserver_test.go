package postgres

import (
	"context"
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

func TestDeleteOrganisation(t *testing.T) {
	metadata, err := structpb.NewStruct(map[string]any{"source": "test"})
	require.NoError(t, err)

	orgResp, err := ac.CreateOrganisation(context.Background(), &pb.CreateOrganisationRequest{
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
		Metadata:               &structpb.Struct{},
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
				dbLPG, err := ac.GetLocationPolicyGroup(t.Context(), &pb.GetLocationPolicyGroupRequest{
					LocationPolicyGroupName: lpResp.Name,
				})
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

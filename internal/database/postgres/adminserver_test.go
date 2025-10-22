package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

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
				OrgName:  "TEST_CREATE_ORGANISATION_1",
				Metadata: metadata,
			},
		},
		{
			name: "Shouldn't create organisation with duplicate name",
			createReq: &pb.CreateOrganisationRequest{
				OrgName:  "TEST_CREATE_ORGANISATION_1",
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
				OrgName:  "TEST_CREATE_ORGANISATION_2",
				Metadata: metadata,
			},
		},
		{
			name: "Should create organisation with empty metadata",
			createReq: &pb.CreateOrganisationRequest{
				OrgName:  "TEST_CREATE_ORGANISATION_3",
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
		OrgName:  "TEST_UPDATE_ORGANISATION",
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
				NewName:  "TEST_UPDATE_ORGANISATION_UPDATED",
				Metadata: metadata2,
			},
			expectedName:     "TEST_UPDATE_ORGANISATION_UPDATED",
			expectedMetadata: metadata2,
		},
		{
			name: "Should update only organisation name if metadata is nil",
			updateReq: &pb.UpdateOrganisationRequest{
				OrgName:  "TEST_UPDATE_ORGANISATION_UPDATED",
				NewName:  "TEST_UPDATE_ORGANISATION_NAME_ONLY",
				Metadata: nil,
			},
			expectedName:     "TEST_UPDATE_ORGANISATION_NAME_ONLY",
			expectedMetadata: metadata2,
		},
		{
			name: "Should update only metadata if name is empty",
			updateReq: &pb.UpdateOrganisationRequest{
				OrgName:  "TEST_UPDATE_ORGANISATION_NAME_ONLY",
				Metadata: metadata1,
			},
			expectedName:     "TEST_UPDATE_ORGANISATION_NAME_ONLY",
			expectedMetadata: metadata1,
		},
		{
			name: "Shouldn't update non-existent organisation",
			updateReq: &pb.UpdateOrganisationRequest{
				OrgName:  "non_existent_org_id",
				NewName:  "SHOULD_NOT_UPDATE",
				Metadata: metadata1,
			},
		},
		{
			name: "Should do nothing if both name and metadata are empty",
			updateReq: &pb.UpdateOrganisationRequest{
				OrgName: "TEST_UPDATE_ORGANISATION_NAME_ONLY",
			},
			expectedName:     "TEST_UPDATE_ORGANISATION_NAME_ONLY",
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
		OrgName:  "TEST_DELETE_ORGANISATION",
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
					OrgName: "TEST_DELETE_ORGANISATION",
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
		OrgName:  "TEST_CREATE_USER_ORGANISATION",
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
		OrgName:  "TEST_DELETE_USER_ORGANISATION",
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

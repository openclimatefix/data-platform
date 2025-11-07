/*= Queries for the IAM schema ================================================================= */

/*- Org Table -----------------------------------------------------------------------------------*/

-- name: CreateOrg :one
INSERT INTO iam.orgs (org_name, metadata)
VALUES (
    LOWER(sqlc.arg(org_name)::TEXT),
    CASE WHEN sqlc.arg(metadata)::JSONB = '{}'::JSONB THEN NULL ELSE sqlc.arg(metadata)::JSONB END
)
RETURNING org_uuid, org_name, metadata;

-- name: UpdateOrg :one
UPDATE iam.orgs
SET
    org_name = LOWER(sqlc.arg(new_org_name)::TEXT),
    metadata = CASE WHEN sqlc.arg(metadata)::JSONB = '{}'::JSONB THEN NULL ELSE sqlc.arg(metadata)::JSONB END
WHERE org_uuid = $1
RETURNING org_uuid, org_name, metadata;

-- name: GetOrgByName :one
SELECT
    od.org_uuid,
    od.org_name,
    od.created_at_utc,
    od.user_uuids,
    od.oauth_ids,
    od.location_policy_group_uuids,
    od.location_policy_group_names,
    od.metadata
FROM iam.org_details_v AS od
WHERE org_name = LOWER(sqlc.arg(org_name)::TEXT);

-- name: ListOrgs :many
SELECT
    org_uuid,
    org_name,
    UUIDV7_EXTRACT_TIMESTAMP(org_uuid)::TIMESTAMP AS created_at_utc,
    metadata
FROM iam.orgs
ORDER BY org_name;

-- name: DeleteOrgByName :exec
DELETE FROM iam.orgs
WHERE org_name = LOWER(sqlc.arg(org_name)::TEXT);

-- name: AddUserToOrgByOAuthIDAndName :exec
INSERT INTO iam.users (org_uuid, oauth_id)
VALUES (
    (
        SELECT o.org_uuid FROM iam.orgs AS o
        WHERE o.org_name = LOWER(sqlc.arg(org_name)::TEXT)
    ),
    sqlc.arg(oauth_id)::TEXT
)
ON CONFLICT DO NOTHING;

-- name: RemoveUserFromOrgByOAuthIDAndName :exec
DELETE FROM iam.users
WHERE org_uuid = (
        SELECT o.org_uuid FROM iam.orgs AS o
        WHERE o.org_name = LOWER(sqlc.arg(org_name)::TEXT)
    )
    AND oauth_id = sqlc.arg(oauth_id)::TEXT;

/*- Users Table ---------------------------------------------------------------------------------*/

-- name: CreateUser :one
INSERT INTO iam.users (org_uuid, oauth_id, metadata)
VALUES (
    $1,
    $2,
    CASE WHEN sqlc.arg(metadata)::JSONB = '{}'::JSONB THEN NULL ELSE sqlc.arg(metadata)::JSONB END
)
RETURNING user_uuid, org_uuid, oauth_id, metadata;

-- name: GetUserByOAuthID :one
SELECT
    u.user_uuid,
    u.org_uuid,
    o.org_name,
    UUIDV7_EXTRACT_TIMESTAMP(u.user_uuid)::TIMESTAMP AS created_at_utc,
    u.oauth_id,
    u.metadata
FROM iam.orgs AS o
    INNER JOIN iam.users AS u USING (org_uuid)
WHERE u.oauth_id = $1;

-- name: DeleteUser :exec
DELETE FROM iam.users
WHERE user_uuid = $1;

/*- Location Policy Groups ----------------------------------------------------------------------*/

-- name: CreateLocationPolicyGroup :one
INSERT INTO iam.location_policy_groups (location_policy_group_name)
VALUES ($1)
RETURNING location_policy_group_uuid, location_policy_group_name;

-- name: UpdateLocationPolicyGroup :one
UPDATE iam.location_policy_groups
SET location_policy_group_name = $2
WHERE location_policy_group_uuid = $1
RETURNING location_policy_group_uuid, location_policy_group_name;

-- name: GetLocationPolicyGroupByUUID :one
SELECT
    location_policy_group_uuid,
    location_policy_group_name
FROM iam.location_policy_groups
WHERE location_policy_group_uuid = $1;

-- name: GetLocationPolicyGroupByName :one
SELECT
    location_policy_group_uuid,
    location_policy_group_name
FROM iam.location_policy_groups
WHERE location_policy_group_name = LOWER(sqlc.arg(location_policy_group_name)::TEXT);

-- name: ListLocationPolicyGroups :many
SELECT
    location_policy_group_uuid,
    location_policy_group_name
FROM iam.location_policy_groups
ORDER BY location_policy_group_name;

-- name: DeleteLocationPolicyGroup :exec
DELETE FROM iam.location_policy_groups
WHERE location_policy_group_uuid = $1;

-- name: AddLocationPolicyGroupToOrgByNames :exec
INSERT INTO iam.org_location_policy_groups (org_uuid, location_policy_group_uuid)
VALUES (
    (
        SELECT o.org_uuid FROM iam.orgs AS o
        WHERE o.org_name = LOWER(sqlc.arg(org_name)::TEXT)
    ),
    (
        SELECT lpg.location_policy_group_uuid FROM iam.location_policy_groups AS lpg
        WHERE lpg.location_policy_group_name = LOWER(sqlc.arg(location_policy_group_name)::TEXT)
    )
)
ON CONFLICT DO NOTHING;

-- name: RemoveLocationPolicyGroupFromOrgByNames :exec
DELETE FROM iam.org_location_policy_groups
WHERE org_uuid = (
        SELECT o.org_uuid FROM iam.orgs AS o
        WHERE o.org_name = LOWER(sqlc.arg(org_name)::TEXT)
    )
    AND location_policy_group_uuid = (
        SELECT lpg.location_policy_group_uuid FROM iam.location_policy_groups AS lpg
        WHERE lpg.location_policy_group_name = LOWER(sqlc.arg(location_policy_group_name)::TEXT)
    );

/*- Location Policies ---------------------------------------------------------------------------*/

-- name: ListLocationPoliciesByGroup :many
SELECT
    lp.permission_id,
    r.permission_name,
    lp.source_type_id,
    st.source_type_name,
    lp.geometry_uuid,
    lp.location_policy_group_uuid
FROM iam.location_policies AS lp
    INNER JOIN iam.permissions AS r USING (permission_id)
    INNER JOIN loc.source_types AS st USING (source_type_id)
WHERE lp.location_policy_group_uuid = $1;

-- name: AddLocationPolicesToGroup :exec
INSERT INTO iam.location_policies (
    permission_id,
    source_type_id,
    geometry_uuid,
    location_policy_group_uuid
) SELECT
    $1,
    $2,
    loc_uuid,
    (
        SELECT lpg.location_policy_group_uuid FROM iam.location_policy_groups AS lpg
        WHERE lpg.location_policy_group_name = sqlc.arg(location_policy_group_name)::TEXT
    )
FROM UNNEST(ARRAY[sqlc.arg(geometry_uuids)::UUID []]) AS t (loc_uuid)
ON CONFLICT DO NOTHING;

-- name: RemoveLocationPoliciesFromGroup :exec
DELETE FROM iam.location_policies
WHERE location_policy_group_uuid = (
        SELECT lpg.location_policy_group_uuid FROM iam.location_policy_groups AS lpg
        WHERE lpg.location_policy_group_name = sqlc.arg(location_policy_group_name)::TEXT
    )
    AND geometry_uuid = $1
    AND source_type_id = $2
    AND permission_id = $3;

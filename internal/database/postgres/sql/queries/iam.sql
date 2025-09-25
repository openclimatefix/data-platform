/*= Queries for the IAM schema ================================================================= */

/*- Org Table -----------------------------------------------------------------------------------*/

-- name: CreateOrg :one
INSERT INTO iam.orgs (org_name, metadata)
VALUES (
    $1,
    CASE WHEN sqlc.arg(metadata)::jsonb = '{}'::jsonb THEN NULL ELSE sqlc.arg(metadata)::jsonb END
)
RETURNING org_uuid, org_name, metadata;

-- name: UpdateOrg :one
UPDATE iam.orgs
SET
    org_name = $2,
    metadata = CASE WHEN sqlc.arg(metadata)::jsonb = '{}'::jsonb THEN NULL ELSE sqlc.arg(metadata)::jsonb END
WHERE org_uuid = $1
RETURNING org_uuid, org_name, metadata;

-- name: GetOrgByUUID :one
SELECT
    od.org_uuid,
    od.org_name,
    od.created_at_utc,
    od.user_uuids,
    od.location_policy_group_uuids,
    od.metadata
FROM iam.org_details_v AS od
WHERE org_uuid = $1
ORDER BY org_uuid;

-- name: GetOrgByName :one
SELECT
    od.org_uuid,
    od.org_name,
    od.created_at_utc,
    od.user_uuids,
    od.location_policy_group_uuids,
    od.metadata
FROM iam.org_details_v AS od
WHERE org_name = $1;

-- name: ListOrgs :many
SELECT
    org_uuid,
    org_name,
    uuidv7_extract_timestamp(org_uuid)::TIMESTAMP AS created_at_utc,
    metadata
FROM iam.orgs
ORDER BY org_name;

-- name: DeleteOrg :exec
DELETE FROM iam.orgs
WHERE org_uuid = $1;

/*- Users Table ---------------------------------------------------------------------------------*/

-- name: CreateUser :one
INSERT INTO iam.users (org_uuid, oauth_id, metadata)
VALUES (
    $1,
    $2,
    CASE WHEN sqlc.arg(metadata)::jsonb = '{}'::jsonb THEN NULL ELSE sqlc.arg(metadata)::jsonb END
)
RETURNING user_uuid, org_uuid, oauth_id, metadata;

-- name: UpdateUser :one
UPDATE iam.users
SET
    org_uuid = $2,
    oauth_id = $3,
    metadata = CASE WHEN sqlc.arg(metadata)::jsonb = '{}'::jsonb THEN NULL ELSE sqlc.arg(metadata)::jsonb END
WHERE user_uuid = $1
RETURNING user_uuid, org_uuid, oauth_id, metadata;

-- name: GetUserByUUID :one
SELECT
    u.user_uuid,
    u.uuidv7_extract_timestamp(u.user_uuid)::TIMESTAMP AS created_at_utc,
    u.org_uuid,
    o.org_name,
    u.oauth_id,
    u.metadata
FROM iam.users AS u
INNER JOIN iam.orgs AS o USING (org_uuid)
WHERE user_uuid = $1;

-- name: GetUserByOAuthID :one
SELECT
    u.user_uuid,
    u.org_uuid,
    o.org_name,
    u.uuidv7_extract_timestamp(u.user_uuid)::TIMESTAMP AS created_at_utc,
    u.oauth_id,
    u.metadata
FROM iam.users AS u
INNER JOIN iam.orgs AS o USING (org_uuid)
WHERE oauth_id = $1;

-- name: ListUsers :many
SELECT
    u.user_uuid,
    o.org_uuid,
    o.org_name,
    u.uuidv7_extract_timestamp(u.user_uuid)::TIMESTAMP AS created_at_utc,
    u.oauth_id,
    u.metadata
FROM iam.users AS u
INNER JOIN iam.orgs AS o USING (org_uuid)
ORDER BY o.org_name;

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
WHERE location_policy_group_name = $1;

-- name: ListLocationPolicyGroups :many
SELECT
    location_policy_group_uuid,
    location_policy_group_name
FROM iam.location_policy_groups
ORDER BY location_policy_group_name;

-- name: DeleteLocationPolicyGroup :exec
DELETE FROM iam.location_policy_groups
WHERE location_policy_group_uuid = $1;

-- name: AddLocationPolicyGroupToOrg :exec
INSERT INTO iam.org_location_policy_groups (org_uuid, location_policy_group_uuid)
VALUES ($1, $2)
ON CONFLICT DO NOTHING;

-- name: RemoveLocationPolicyGroupFromOrg :exec
DELETE FROM iam.org_location_policy_groups
WHERE org_uuid = $1
    AND location_policy_group_uuid = $2;

/*- Location Policies ---------------------------------------------------------------------------*/

-- name: AddLocationPolicesToGroup :exec
INSERT INTO iam.location_policies (
    role_id,
    source_type_id,
    location_uuid,
    location_policy_group_uuid
) SELECT
    (SELECT r.role_id FROM iam.roles AS r WHERE r.role_name = sqlc.arg(role_name)::text),
    (SELECT st.source_type_id FROM loc.source_types AS st WHERE st.source_type_name = sqlc.arg(source_type_name)::text),
    loc_uuid,
    (SELECT lpg.location_policy_group_uuid FROM iam.location_policy_groups AS lpg WHERE lpg.location_policy_group_name = sqlc.arg(location_policy_group_name)::text)
FROM UNNEST(ARRAY[sqlc.arg(location_uuids)::uuid []]) AS t (loc_uuid)
ON CONFLICT DO NOTHING;

-- name: DeleteLocationPoliciesFromGroup :exec
DELETE FROM iam.location_policies
WHERE location_policy_group_uuid = $1
AND location_uuid = ANY($2::UUID[])
AND source_type_id = (SELECT st.source_type_id FROM loc.source_types AS st WHERE st.source_type_name = $3)
AND role_id = (SELECT r.role_id FROM iam.roles AS r WHERE r.role_name = $4);

/*- Materialized Views ---------------------------------------------------------------------------*/

-- name: UpdateUserLocationPoliciesMaterializedView :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY iam.user_location_policies_mv;

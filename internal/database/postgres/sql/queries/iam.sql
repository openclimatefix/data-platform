/*= Queries for the IAM schema ================================================================= */

/*- Org Table -----------------------------------------------------------------------------------*/

-- name: CreateOrg :one
INSERT INTO iam.orgs (org_name, metadata)
VALUES (
    $1,
    CASE WHEN sqlc.arg(metadata)::JSONB = '{}'::JSONB THEN NULL ELSE sqlc.arg(metadata)::JSONB END
)
RETURNING org_uuid, org_name, metadata;

-- name: UpdateOrg :one
UPDATE iam.orgs
SET
    org_name = $2,
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
WHERE org_name = $1;

-- name: ListOrgs :many
SELECT
    org_uuid,
    org_name,
    UUIDV7_EXTRACT_TIMESTAMP(org_uuid)::TIMESTAMP AS created_at_utc,
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
    CASE WHEN sqlc.arg(metadata)::JSONB = '{}'::JSONB THEN NULL ELSE sqlc.arg(metadata)::JSONB END
)
RETURNING user_uuid, org_uuid, oauth_id, metadata;

-- name: UpdateUser :one
UPDATE iam.users
SET
    org_uuid = $2,
    oauth_id = $3,
    metadata = CASE WHEN sqlc.arg(metadata)::JSONB = '{}'::JSONB THEN NULL ELSE sqlc.arg(metadata)::JSONB END
WHERE user_uuid = $1
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

-- name: GetUserLocations :many
/* GetUserLocations returns all locations that the service account has access to.
 */
SELECT
    l.location_uuid,
    l.location_name,
    ulp.source_type_id,
    ulp.role_name,
    ST_Y(l.centroid)::REAL AS latitude,
    ST_X(l.centroid)::REAL AS longitude
FROM loc.locations AS l
    INNER JOIN iam.user_location_policies_mv AS ulp USING (location_uuid)
WHERE ulp.user_uuid = $1
    AND ulp.role_id IN (1, 2)
ORDER BY l.location_name;

-- name: FilterLocationsByUser :many
/* FilterLocationsByOAuthID returns the intersection of the locations accessible by the user
 * (identified by the given OAuth ID), and the provided list of location UUIDs.
 */
SELECT
    location_uuid
FROM iam.user_location_policies_mv
WHERE user_uuid = $1
    AND role_id = ANY(sqlc.arg(role_id)::TEXT [])
    AND source_type_id = $2
    AND location_uuid = ANY(sqlc.arg(unfiltered_location_uuids)::UUID []);

-- name: ListUsers :many
SELECT
    u.user_uuid,
    o.org_uuid,
    o.org_name,
    UUIDV7_EXTRACT_TIMESTAMP(u.user_uuid)::TIMESTAMP AS created_at_utc,
    u.oauth_id,
    u.metadata
FROM iam.users AS u
    INNER JOIN iam.orgs AS o USING (org_uuid)
ORDER BY o.org_name;

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

-- name: AddLocationPolicyGroupsToOrg :exec
INSERT INTO iam.org_location_policy_groups (org_uuid, location_policy_group_uuid)
SELECT
    sqlc.arg(org_uuid)::UUID,
    lpg.location_policy_group_uuid
FROM UNNEST(ARRAY[sqlc.arg(location_policy_group_uuids)::UUID []]) AS t (location_policy_group_uuid)
    INNER JOIN iam.location_policy_groups AS lpg USING (location_policy_group_uuid)
ON CONFLICT DO NOTHING;

-- name: RemoveLocationPolicyGroupsFromOrg :exec
DELETE FROM iam.org_location_policy_groups
WHERE org_uuid = $1
    AND location_policy_group_uuid = ANY(sqlc.arg(location_policy_group_uuids)::UUID []);

/*- Location Policies ---------------------------------------------------------------------------*/

-- name: ListLocationPoliciesByGroup :many
SELECT
    lp.role_id,
    r.role_name,
    lp.source_type_id,
    st.source_type_name,
    lp.location_uuid,
    lp.location_policy_group_uuid
FROM iam.location_policies AS lp
    INNER JOIN iam.roles AS r USING (role_id)
    INNER JOIN loc.source_types AS st USING (source_type_id)
WHERE lp.location_policy_group_uuid = $1;

-- name: AddLocationPolicesToGroup :exec
INSERT INTO iam.location_policies (
    role_id,
    source_type_id,
    location_uuid,
    location_policy_group_uuid
) SELECT
    (
        SELECT r.role_id FROM iam.roles AS r
        WHERE r.role_name = sqlc.arg(role_name)::TEXT
    ),
    (
        SELECT st.source_type_id FROM loc.source_types AS st
        WHERE st.source_type_name = sqlc.arg(source_type_name)::TEXT
    ),
    loc_uuid,
    (
        SELECT lpg.location_policy_group_uuid FROM iam.location_policy_groups AS lpg
        WHERE lpg.location_policy_group_name = sqlc.arg(location_policy_group_name)::TEXT
    )
FROM UNNEST(ARRAY[sqlc.arg(location_uuids)::UUID []]) AS t (loc_uuid)
ON CONFLICT DO NOTHING;

-- name: DeleteLocationPoliciesFromGroup :exec
DELETE FROM iam.location_policies
WHERE location_policy_group_uuid = $1
    AND location_uuid = ANY($2::UUID [])
    AND source_type_id = (
        SELECT st.source_type_id FROM loc.source_types AS st
        WHERE st.source_type_name = $3
    )
    AND role_id = (
        SELECT r.role_id FROM iam.roles AS r
        WHERE r.role_name = $4
    );

-- name: DeleteAllLocationPoliciesFromGroup :exec
DELETE FROM iam.location_policies
WHERE location_policy_group_uuid = $1;

/*- Materialized Views ---------------------------------------------------------------------------*/

-- name: RefreshUserLocationPoliciesMaterializedView :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY iam.user_location_policies_mv;

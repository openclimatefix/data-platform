/* --- Queries for the IAM table --- */

-- name: GetLocationPolicy :many
SELECT (
    role_name,
    location_uuid,
    service_account
) FROM iam.location_policies
INNER JOIN iam.roles USING (role_id)
WHERE service_account = $1
    AND role_name = ANY(sqlc.arg(role_names)::text [])
    AND location_uuid = $2;

-- name: ListLocationPolicies :many
SELECT (
    role_name,
    location_uuid,
    service_account
) FROM iam.location_policies
WHERE service_account = $1
    AND role_name = ANY(sqlc.arg(role_names)::text []);

-- name: CreateLocationPolices :exec
INSERT INTO iam.location_policies (
    role_id,
    service_account,
    location_uuid
) SELECT
    (SELECT r.role_id FROM iam.roles AS r WHERE r.role_name = sqlc.arg(role_name)::text),
    $1,
    loc_uuid
FROM UNNEST(ARRAY[sqlc.arg(location_uuids)::uuid []]) AS t (loc_uuid)
ON CONFLICT DO NOTHING;

-- name: DeleteLocationPolicies :exec
DELETE FROM iam.location_policies
WHERE service_account = $1
    AND location_uuid = ANY(sqlc.arg(location_uuids)::uuid []);

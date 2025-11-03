/*- Queries for the locations table ------------------------------ */

-- name: CreateLocation :one
INSERT INTO loc.locations AS l (
    location_name, geom, location_type_id
) VALUES (
    LOWER(sqlc.arg(location_name)::TEXT),
    ST_GEOMFROMTEXT(sqlc.arg(geom)::TEXT, 4326), --Ensure in WSG84
    $1
) RETURNING l.location_uuid, l.location_name;

-- name: GetLocationGeoJSON :one
/* GetLocationGeoJSON returns a GeoJSON FeatureCollection for the given locations.
 * The simplification level can be adjusted via the `simplification_level` argument.
 */
SELECT
    JSON_BUILD_OBJECT(
        'type', 'FeatureCollection',
        'features', JSON_AGG(
            ST_ASGEOJSON(
                sl.*, id_column => 'location_uuid'::TEXT, geom_column => 'geom_simple'
            )::JSONB
        )
    ) AS geojson
FROM (
    SELECT
        l.location_uuid,
        l.location_name,
        l.location_type_id,
        ST_SIMPLIFYPRESERVETOPOLOGY(l.geom, sqlc.arg(simplification_level)::REAL) AS geom_simple
    FROM loc.locations AS l
    WHERE l.location_uuid = ANY(sqlc.arg(location_uuids)::UUID [])
) AS sl;

/*- Queries for the sources table -------------------------------------*/

-- name: GetLocationSourceAtTimestamp :one
/* GetLocationSourceAtTimestamp returns the source for a given location and source type at a
 * specific timestamp.
 */
SELECT
    s.capacity,
    s.capacity_limit_sip,
    s.capacity_unit_prefix_factor,
    COALESCE(
        s.capacity_limit_sip::REAL * s.capacity / 30000.0, s.capacity::REAL
    )::REAL AS capacity_inc_limit,
    s.source_type_id,
    s.metadata AS metadata_jsonb,
    s.location_uuid,
    l.location_name,
    ST_X(l.centroid)::REAL AS longitude,
    ST_Y(l.centroid)::REAL AS latitude
FROM loc.sources_mv AS s
    INNER JOIN loc.locations AS l USING (location_uuid)
WHERE
    l.location_uuid = $1
    AND s.source_type_id = $2
    AND s.sys_period @> sqlc.arg(at_timestamp_utc)::TIMESTAMP;

-- name: ListSourcesAtTimestamp :many
/* ListSourcesAtTimestamp returns all sources for a given location name and source type.
 * If just querying for one source, it will be faster to use GetLocationSourceAtTimestamp.
 */
SELECT
    COALESCE(
        sh.capacity_limit_sip::REAL * sh.capacity / 30000.0, sh.capacity::REAL
    )::REAL AS effective_capacity,
    s.capacity_unit_prefix_factor,
    s.source_type_id,
    s.metadata AS metadata_jsonb,
    s.location_uuid,
    l.location_name,
    ST_X(l.centroid)::REAL AS longitude,
    ST_Y(l.centroid)::REAL AS latitude
FROM loc.sources_mv AS s
    INNER JOIN loc.locations AS l USING (location_uuid)
WHERE
    l.location_uuid = ANY(sqlc.arg(location_uuids)::UUID [])
    AND s.source_type_id = $1
    AND s.sys_period @> sqlc.arg(at_timestamp_utc)::TIMESTAMP;

-- name: CreateLocationSourceEntry :one
/* CreateLocationSourceEntry creates a new source entry for a given location and source type.
 */
INSERT INTO loc.sources_history (
    location_uuid,
    source_type_id,
    capacity,
    capacity_unit_prefix_factor,
    capacity_limit_sip,
    valid_from_utc,
    metadata
) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    CASE WHEN sqlc.arg(metadata)::JSONB = '{}'::JSONB THEN NULL ELSE sqlc.arg(metadata)::JSONB END
) RETURNING location_uuid, capacity, capacity_unit_prefix_factor;

-- name: RefreshSourcesMaterializedView :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY loc.sources_mv;

-- name: DecommissionSource :exec
/* DecommissionSource creates a new source entry for a given location and source type with 0 capacity.
 */
INSERT INTO loc.sources_history (
    location_uuid,
    source_type_id,
    capacity,
    capacity_unit_prefix_factor,
    capacity_limit_sip,
    valid_from_utc,
    metadata
) VALUES (
    $1,
    $2,
    0,
    0,
    NULL,
    DATE_TRUNC('minute', CURRENT_TIMESTAMP),
    NULL
);

-- name: GetLocationSourceHistoryTimeseries :many
/* GetLocationSourceHistoryTimeseries shows all the historical records for a given location and source type. */
SELECT
    sh.capacity,
    sh.capacity_unit_prefix_factor,
    sh.capacity_limit_sip,
    sh.valid_from_utc
FROM loc.sources_history AS sh
WHERE sh.location_uuid = $1 AND sh.source_type_id = $2
ORDER BY valid_from_utc DESC;

/*- Compound Queries for locations and policies -------------------------------------------------*/

-- name: GetLocationsByFilters :many
/* GetLocationsByFilters returns all locations that match the given filters.
 * It uses left joins to include locations even if there are no associated policies, to allow the
 * caller to not include permission-based filtering.
 */
WITH all_locations AS (
    SELECT
        u.oauth_id,
        lp.permission_id,
        ls.source_type_id,
        ls.location_uuid,
        ls.capacity,
        ls.capacity_unit_prefix_factor,
        l.location_name,
        l.location_type_id,
        ST_X(l.centroid)::REAL AS longitude,
        ST_Y(l.centroid)::REAL AS latitude
    FROM loc.sources_mv AS ls
        INNER JOIN loc.locations AS l USING (location_uuid)
        LEFT OUTER JOIN iam.location_policies AS lp USING (location_uuid, source_type_id)
        LEFT OUTER JOIN iam.org_location_policy_groups USING (location_policy_group_uuid)
        LEFT OUTER JOIN iam.users AS u USING (org_uuid)
    WHERE ls.sys_period @> sqlc.arg(at_timestamp_utc)::TIMESTAMP
)
SELECT * FROM all_locations AS al
WHERE
    (sqlc.narg(source_type_id)::SMALLINT IS NULL OR al.source_type_id = sqlc.narg(source_type_id)::SMALLINT)
    AND (
        ARRAY_LENGTH(sqlc.arg(location_uuids)::UUID [], 1) IS NULL
        OR al.location_uuid = ANY(sqlc.arg(location_uuids)::UUID [])
    )
    AND (sqlc.narg(location_type_id)::SMALLINT IS NULL OR al.location_type_id = sqlc.narg(location_type_id)::SMALLINT)
    AND (sqlc.narg(oauth_id)::TEXT IS NULL OR al.oauth_id = sqlc.arg(oauth_id)::TEXT)
    AND (sqlc.narg(permission_id)::SMALLINT IS NULL OR al.permission_id = sqlc.narg(permission_id)::SMALLINT);

-- name: GetLocationsByFiltersWithinLocation :many
/* GetLocationsByFiltersWithinLocation returns all locations that match the given filters
 * and are within the geometry of the given location.
 * This has to be seperated from the GetLocationsByFilters query due to the spatial join.
 */
WITH contained_locations AS (
    SELECT
        l.location_uuid,
        l.location_name,
        l.location_type_id,
        l.geom,
        l.centroid
    FROM loc.locations AS l
        INNER JOIN
            loc.locations AS l_outer ON ST_WITHIN(
                l.geom,
                l_outer.geom
            ) AND l_outer.location_uuid = sqlc.arg(outer_location_uuid)::UUID
            AND l.location_uuid <> sqlc.arg(outer_location_uuid)::UUID
),
all_locations AS (
    SELECT
        u.oauth_id,
        lp.permission_id,
        ls.source_type_id,
        ls.location_uuid,
        ls.capacity,
        ls.capacity_unit_prefix_factor,
        l.location_name,
        l.location_type_id,
        ST_X(l.centroid)::REAL AS longitude,
        ST_Y(l.centroid)::REAL AS latitude
    FROM loc.sources_mv AS ls
        INNER JOIN contained_locations AS l USING (location_uuid)
        LEFT OUTER JOIN iam.location_policies AS lp USING (location_uuid, source_type_id)
        LEFT OUTER JOIN iam.org_location_policy_groups USING (location_policy_group_uuid)
        LEFT OUTER JOIN iam.users AS u USING (org_uuid)
    WHERE ls.sys_period @> sqlc.arg(at_timestamp_utc)::TIMESTAMP
)
SELECT * FROM all_locations AS al
WHERE
    (sqlc.narg(source_type_id)::SMALLINT IS NULL OR al.source_type_id = sqlc.narg(source_type_id)::SMALLINT)
    AND (
        ARRAY_LENGTH(sqlc.arg(location_uuids)::UUID [], 1) IS NULL
        OR al.location_uuid = ANY(sqlc.arg(location_uuids)::UUID [])
    )
    AND (sqlc.narg(location_type_id)::SMALLINT IS NULL OR al.location_type_id = sqlc.narg(location_type_id)::SMALLINT)
    AND (sqlc.narg(oauth_id)::TEXT IS NULL OR al.oauth_id = sqlc.arg(oauth_id)::TEXT)
    AND (sqlc.narg(permission_id)::SMALLINT IS NULL OR al.permission_id = sqlc.narg(permission_id)::SMALLINT);

/*- Queries for the geometries table ------------------------------ */

-- name: CreateGeometry :one
INSERT INTO loc.geometries AS l (
    geometry_name, geom, geometry_type_id
) VALUES (
    LOWER(sqlc.arg(geometry_name)::TEXT),
    ST_GEOMFROMTEXT(sqlc.arg(geom)::TEXT, 4326), --Ensure in WSG84
    $1
) RETURNING l.geometry_uuid, l.geometry_name;

-- name: GetGeometryGeoJSON :one
/* GetLocationGeoJSON returns a GeoJSON FeatureCollection for the given geometries.
 * The simplification level can be adjusted via the `simplification_level` argument.
 */
SELECT
    JSON_BUILD_OBJECT(
        'type', 'FeatureCollection',
        'features', JSON_AGG(
            ST_ASGEOJSON(
                sl.*, id_column => 'geometry_uuid'::TEXT, geom_column => 'geom_simple'
            )::JSONB
        )
    ) AS geojson
FROM (
    SELECT
        l.geometry_uuid,
        l.geometry_name,
        l.geometry_type_id,
        ST_SIMPLIFYPRESERVETOPOLOGY(l.geom, sqlc.arg(simplification_level)::REAL) AS geom_simple
    FROM loc.geometries AS l
    WHERE l.geometry_uuid = ANY(sqlc.arg(geometry_uuids)::UUID [])
) AS sl;

/*- Queries for the sources table -------------------------------------*/

-- name: GetSourceAtTimestamp :one
/* GetSourceAtTimestamp returns the source for a given geometry and source type at a
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
    s.geometry_uuid,
    l.geometry_name,
    ST_X(l.centroid)::REAL AS longitude,
    ST_Y(l.centroid)::REAL AS latitude
FROM loc.sources_mv AS s
    INNER JOIN loc.geometries AS l USING (geometry_uuid)
WHERE
    l.geometry_uuid = $1
    AND s.source_type_id = $2
    AND s.sys_period @> sqlc.arg(at_timestamp_utc)::TIMESTAMP;

-- name: CreateSourceEntry :one
/* CreateSourceEntry creates a new source entry for a given geometry and source type.
 */
INSERT INTO loc.sources_history (
    geometry_uuid,
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
) RETURNING geometry_uuid, capacity, capacity_unit_prefix_factor;

-- name: RefreshSourcesMaterializedView :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY loc.sources_mv;

-- name: DecommissionSource :exec
/* DecommissionSource creates a new source entry for a given geometry and source type with 0 capacity.
 */
INSERT INTO loc.sources_history (
    geometry_uuid,
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

-- name: GetSourceHistory :many
/* GetSourceHistory shows all the historical records for a given geometry and source type. */
SELECT
    sh.capacity,
    sh.capacity_unit_prefix_factor,
    sh.capacity_limit_sip,
    sh.valid_from_utc
FROM loc.sources_history AS sh
WHERE sh.geometry_uuid = $1 AND sh.source_type_id = $2
ORDER BY valid_from_utc DESC;

-- name: ListSourcesAtTimestamp :many
/* ListSourcesAtTimestamp returns all sources that match the given filters.
 * It uses left joins to include geometries even if there are no associated policies, to allow the
 * caller to not include permission-based filtering.
 */
WITH unfiltered_sources AS (
    SELECT
        u.oauth_id,
        lp.permission_id,
        ls.source_type_id,
        ls.geometry_uuid,
        ls.capacity,
        ls.capacity_unit_prefix_factor,
        ls.capacity_limit_sip,
        l.geometry_name,
        l.geometry_type_id,
        ST_X(l.centroid)::REAL AS longitude,
        ST_Y(l.centroid)::REAL AS latitude,
        l.metadata AS geometry_metadata,
        ls.metadata AS source_metadata
    FROM loc.sources_mv AS ls
        INNER JOIN loc.geometries AS l USING (geometry_uuid)
        LEFT OUTER JOIN iam.location_policies AS lp USING (geometry_uuid, source_type_id)
        LEFT OUTER JOIN iam.org_location_policy_groups USING (location_policy_group_uuid)
        LEFT OUTER JOIN iam.users AS u USING (org_uuid)
    WHERE ls.sys_period @> sqlc.arg(at_timestamp_utc)::TIMESTAMP
)
SELECT
    *,
    COALESCE(
        us.capacity_limit_sip::REAL * us.capacity / 30000.0, us.capacity::REAL
    )::REAL AS capacity_inc_limit
FROM unfiltered_sources AS us
WHERE
    (sqlc.narg(source_type_id)::SMALLINT IS NULL OR us.source_type_id = sqlc.narg(source_type_id)::SMALLINT)
    AND (
        ARRAY_LENGTH(sqlc.arg(geometry_uuids)::UUID [], 1) IS NULL
        OR us.geometry_uuid = ANY(sqlc.arg(geometry_uuids)::UUID [])
    )
    AND (sqlc.narg(geometry_type_id)::SMALLINT IS NULL OR us.geometry_type_id = sqlc.narg(geometry_type_id)::SMALLINT)
    AND (sqlc.narg(oauth_id)::TEXT IS NULL OR us.oauth_id = sqlc.arg(oauth_id)::TEXT)
    AND (sqlc.narg(permission_id)::SMALLINT IS NULL OR us.permission_id = sqlc.narg(permission_id)::SMALLINT);

-- name: ListSourcesAtTimestampWithin :many
/* ListSourcesAtTimestampWithin returns all sources that match the given filters
 * and are within a given geometry.
 * This has to be seperated from the ListSourcesAtTimestamp query due to the spatial join.
 */
WITH contained_geometries AS (
    SELECT
        l.geometry_uuid,
        l.geometry_name,
        l.geometry_type_id,
        l.geom,
        l.centroid,
        l.metadata
    FROM loc.geometries AS l
        INNER JOIN
            loc.geometries AS l_outer ON ST_WITHIN(
                l.geom,
                l_outer.geom
            ) AND l_outer.geometry_uuid = sqlc.arg(outer_geometry_uuid)::UUID
            AND l.geometry_uuid <> sqlc.arg(outer_geometry_uuid)::UUID
),
unfiltered_sources AS (
    SELECT
        u.oauth_id,
        lp.permission_id,
        ls.source_type_id,
        ls.geometry_uuid,
        ls.capacity,
        ls.capacity_unit_prefix_factor,
        l.geometry_name,
        l.geometry_type_id,
        ST_X(l.centroid)::REAL AS longitude,
        ST_Y(l.centroid)::REAL AS latitude,
        l.metadata AS geometry_metadata,
        ls.metadata AS source_metadata
    FROM loc.sources_mv AS ls
        INNER JOIN contained_geometries AS l USING (geometry_uuid)
        LEFT OUTER JOIN iam.location_policies AS lp USING (geometry_uuid, source_type_id)
        LEFT OUTER JOIN iam.org_location_policy_groups USING (location_policy_group_uuid)
        LEFT OUTER JOIN iam.users AS u USING (org_uuid)
    WHERE ls.sys_period @> sqlc.arg(at_timestamp_utc)::TIMESTAMP
)
SELECT
    *,
    COALESCE(
        us.capacity_limit_sip::REAL * us.capacity / 30000.0, us.capacity::REAL
    )::REAL AS capacity_inc_limit
FROM unfiltered_sources AS us
WHERE
    (sqlc.narg(source_type_id)::SMALLINT IS NULL OR us.source_type_id = sqlc.narg(source_type_id)::SMALLINT)
    AND (
        ARRAY_LENGTH(sqlc.arg(geometry_uuids)::UUID [], 1) IS NULL
        OR us.geometry_uuid = ANY(sqlc.arg(geometry_uuids)::UUID [])
    )
    AND (sqlc.narg(geometry_type_id)::SMALLINT IS NULL OR us.geometry_type_id = sqlc.narg(geometry_type_id)::SMALLINT)
    AND (sqlc.narg(oauth_id)::TEXT IS NULL OR us.oauth_id = sqlc.arg(oauth_id)::TEXT)
    AND (sqlc.narg(permission_id)::SMALLINT IS NULL OR us.permission_id = sqlc.narg(permission_id)::SMALLINT);

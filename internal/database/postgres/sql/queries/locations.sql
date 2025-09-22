/*- Queries for the locations table ------------------------------ */

-- name: GetSourceTypeByName :one
SELECT
    source_type_id,
    source_type_name
FROM loc.source_types
WHERE source_type_name = $1;

-- name: CreateLocation :one
INSERT INTO loc.locations AS l (
    location_name, geom, location_type_id
) VALUES (
    UPPER(sqlc.arg(location_name)::text),
    ST_GEOMFROMTEXT(sqlc.arg(geom)::text, 4326), --Ensure in WSG84
    (
        SELECT location_type_id FROM loc.location_types AS lt
        WHERE lt.location_type_name = UPPER(sqlc.arg(location_type_name)::text)
    )
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
                sl.*, id_column => 'location_uuid'::text, geom_column => 'geom_simple'
            )::jsonb
        )
    ) AS geojson
FROM (
    SELECT
        l.location_uuid,
        l.location_name,
        lt.location_type_name,
        ST_SIMPLIFYPRESERVETOPOLOGY(l.geom, sqlc.arg(simplification_level)::real) AS geom_simple
    FROM loc.locations AS l
    INNER JOIN loc.location_types AS lt USING (location_type_id)
    WHERE l.location_uuid = ANY(sqlc.arg(location_uuids)::uuid [])
) AS sl;

-- name: GetLocations :many
/* GetLocations returns all locations.
 */
SELECT
    l.location_uuid,
    l.location_name,
    ST_Y(l.centroid)::real AS latitude,
    ST_X(l.centroid)::real AS longitude
FROM loc.locations AS l
ORDER BY l.location_name;


-- name: GetUserLocations :many
/* GetUserLocations returns all locations that the service account has access to.
 */
SELECT
    l.location_uuid,
    l.location_name,
    ST_Y(l.centroid)::real AS latitude,
    ST_X(l.centroid)::real AS longitude
FROM loc.locations AS l
INNER JOIN iam.location_policies AS lp USING (location_uuid)
WHERE lp.service_account = $1
    AND lp.role_id IN (1, 2)
ORDER BY l.location_name;

-- name: GetLocationsWithin :many
/* GetUserLocationsWithin returns all locations that are within the geometry of the given location.
 */
SELECT
    l.location_uuid,
    l.location_name,
    ST_Y(l.centroid)::real AS latitude,
    ST_X(l.centroid)::real AS longitude
FROM loc.locations AS l
INNER JOIN iam.location_policies USING (location_uuid)
INNER JOIN
    loc.locations AS l_outer ON ST_WITHIN(
        l.geom,
        l_outer.geom
    )
WHERE l_outer.location_uuid = $1;

-- name: GetUserLocationsWithin :many
/* GetUserLocationsWithin returns all locations that are within the geometry of the given location
 * that the service account has access to.
 */
SELECT
    l.location_uuid,
    l.location_name,
    ST_Y(l.centroid)::real AS latitude,
    ST_X(l.centroid)::real AS longitude
FROM loc.locations AS l
INNER JOIN iam.location_policies AS lp USING (location_uuid)
INNER JOIN
    loc.locations AS l_outer ON ST_WITHIN(
        l.geom,
        l_outer.geom
    )
WHERE lp.service_account = $2
    AND lp.role_id IN (1, 2)
    AND l_outer.location_uuid = $1;

/*- Queries for the sources table -------------------------------------*/

-- name: GetLocationSourceAtTimestamp :one
/* GetLocationSourceAtTimestamp returns the source for a given location and source type at a
 * specific timestamp.
 */
SELECT
    s.capacity,
    s.capacity_unit_prefix_factor,
    s.capacity_limit_sip,
    s.source_type_id,
    s.metadata AS metadata_jsonb,
    s.location_uuid,
    l.location_name,
    ST_X(l.centroid)::real AS longitude,
    ST_Y(l.centroid)::real AS latitude
FROM loc.sources_mv AS s
INNER JOIN loc.locations AS l USING (location_uuid)
INNER JOIN loc.source_types AS st USING (source_type_id)
WHERE
    l.location_uuid = $1
    AND st.source_type_name = $2
    AND s.sys_period @> sqlc.arg(at_timestamp_utc)::timestamp;

-- name: GetUserLocationSourceAtTimestamp :one
/* GetUserLocationSourceAtTimestamp returns the source for a given location and source type at a
 * specific timestamp, if the user has access to that location.
 */
SELECT
    s.capacity,
    s.capacity_unit_prefix_factor,
    s.capacity_limit_sip,
    s.source_type_id,
    s.metadata AS metadata_jsonb,
    s.location_uuid,
    l.location_name,
    ST_X(l.centroid)::real AS longitude,
    ST_Y(l.centroid)::real AS latitude
FROM iam.location_policies AS lp
INNER JOIN loc.sources_mv AS s USING (location_uuid)
INNER JOIN loc.locations AS l USING (location_uuid)
INNER JOIN loc.source_types AS st USING (source_type_id)
WHERE
    lp.service_account = $3
    AND lp.role_id IN (1, 2)
    AND lp.location_uuid = $1
    AND st.source_type_name = $2
    AND s.sys_period @> sqlc.arg(at_timestamp_utc)::timestamp;

-- name: ListSourcesAtTimestamp :many
/* ListSourcesAtTimestamp returns all sources for a given location name and source type.
 * If just querying for one source, it will be faster to use GetLocationSourceAtTimestamp.
 */
SELECT
    s.capacity,
    s.capacity_unit_prefix_factor,
    s.capacity_limit_sip,
    s.source_type_id,
    s.metadata AS metadata_jsonb,
    s.location_uuid,
    l.location_name,
    ST_X(l.centroid)::real AS longitude,
    ST_Y(l.centroid)::real AS latitude
FROM loc.sources_mv AS s
INNER JOIN loc.locations AS l USING (location_uuid)
INNER JOIN loc.source_types AS st USING (source_type_id)
WHERE
    l.location_uuid = ANY(sqlc.arg(location_uuids)::uuid [])
    AND st.source_type_name = $1
    AND s.sys_period @> sqlc.arg(at_timestamp_utc)::timestamp;

-- name: ListUserLocationSourcesAtTimestamp :many
/* ListUserLocationSourcesAtTimestamp returns all sources for a given source type and set of location
 * uuids that the user has access to.
 * If just querying for one source, it will be faster to use GetUserLocationSourceAtTimestamp.
 */
SELECT
    s.capacity,
    s.capacity_unit_prefix_factor,
    s.capacity_limit_sip,
    s.source_type_id,
    s.metadata AS metadata_jsonb,
    s.location_uuid,
    l.location_name,
    ST_X(l.centroid)::real AS longitude,
    ST_Y(l.centroid)::real AS latitude
FROM iam.location_policies AS lp
INNER JOIN loc.sources_mv AS s USING (location_uuid)
INNER JOIN loc.locations AS l USING (location_uuid)
INNER JOIN loc.source_types AS st USING (source_type_id)
WHERE
    lp.service_account = $2
    AND lp.role_id IN (1, 2)
    AND lp.location_uuid = ANY(sqlc.arg(location_uuids)::uuid [])
    AND st.source_type_name = $1
    AND s.sys_period @> sqlc.arg(at_timestamp_utc)::timestamp;

-- name: CreateUserLocationSourceEntry :one
/* CreateUserLocationSourceEntry creates a new source entry for a given location and source type.
 */
INSERT INTO loc.sources_history (
    location_uuid,
    source_type_id,
    capacity,
    capacity_unit_prefix_factor,
    capacity_limit_sip,
    valid_from_utc,
    metadata
) SELECT
    lp.location_uuid,
    $2,
    $3,
    $4,
    $5,
    $6,
    CASE WHEN sqlc.arg(metadata)::jsonb = '{}'::jsonb THEN NULL ELSE sqlc.arg(metadata)::jsonb END
FROM iam.location_policies lp
WHERE lp.service_account = $7
    AND lp.role_id = 1 -- Have to be owner to create a source
    AND lp.location_uuid = $1
RETURNING location_uuid, capacity, capacity_unit_prefix_factor;

-- name: UpdateSourcesMaterializedView :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY loc.sources_mv;

-- name: DecommissionUserSource :exec
/* DecommissionUserSource creates a new source entry for a given location and source type with 0 capacity.
 */
INSERT INTO loc.sources_history (
    location_uuid,
    source_type_id,
    capacity,
    capacity_unit_prefix_factor,
    capacity_limit_sip,
    valid_from_utc,
    metadata
) SELECT
    lp.location_uuid,
    $2,
    0,
    0,
    NULL,
    CURRENT_TIMESTAMP,
    NULL
FROM iam.location_policies lp
WHERE lp.service_account = $3
    AND lp.role_id = 1 -- Have to be owner to decommission a source
    AND lp.location_uuid = $1;

-- name: GetUserLocationSourceHistoryTimeseries :many
/* GetUserLocationSourceHistoryTimeseries shows all the historical records for a given location
 * and source type, if the user has access to that location.
 */
SELECT
    sh.capacity,
    sh.capacity_unit_prefix_factor,
    sh.capacity_limit_sip,
    sh.valid_from_utc
FROM loc.sources_history AS sh
INNER JOIN iam.location_policies lp USING (location_uuid)
WHERE lp.service_account = $3
    AND lp.role_id IN (1, 2) -- Have to be owner or viewer to see source history
    AND lp.location_uuid = $1
    AND sh.source_type_id = $2
ORDER BY valid_from_utc DESC;

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

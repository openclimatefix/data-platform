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
    JOIN loc.location_types AS lt USING (location_type_id)
    WHERE l.location_uuid = ANY(sqlc.arg(location_uuids)::uuid [])
) AS sl;

-- name: GetLocationsWithin :many
/* GetLocationIdsWithin returns all locations that are within the geometry of the given location.
 */
SELECT
    l.location_uuid,
    l.location_name,
    ST_Y(l.centroid)::real AS latitude,
    ST_X(l.centroid)::real AS longitude
FROM loc.locations AS l
INNER JOIN
    loc.locations AS l_outer ON ST_WITHIN(
        l.geom,
        l_outer.geom
    )
WHERE l_outer.location_uuid = $1;

/*- Queries for the sources table -------------------------------------*/

-- name: GetSourceAtTimestamp :one
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

-- name: ListSourcesAtTimestamp :many
/* ListSources returns all sources for a given location name and source type.
 * If just querying for one source, it will be faster to use GetSource.
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

-- name: CreateSourceEntry :one
INSERT INTO loc.sources_history (
    location_uuid,
    source_type_id,
    capacity,
    capacity_unit_prefix_factor,
    capacity_limit_sip,
    valid_from_utc,
    metadata
) SELECT
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    CASE WHEN sqlc.arg(metadata)::jsonb = '{}'::jsonb THEN NULL ELSE sqlc.arg(metadata)::jsonb END
RETURNING location_uuid, capacity, capacity_unit_prefix_factor;

-- name: UpdateSourcesMaterializedView :exec
REFRESH MATERIALIZED VIEW CONCURRENTLY loc.sources_mv;

-- name: DecomissionSource :exec
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
    CURRENT_TIMESTAMP,
    NULL
);

-- name: GetSourceHistoryTimeseries :many
/* GetSourceHistoryTimeseries shows all the historical records for a given location and source type. */
SELECT
    sh.capacity,
    sh.capacity_unit_prefix_factor,
    sh.capacity_limit_sip,
    sh.valid_from_utc
FROM loc.sources_history AS sh
WHERE sh.location_uuid = $1 AND sh.source_type_id = $2
ORDER BY valid_from_utc DESC;

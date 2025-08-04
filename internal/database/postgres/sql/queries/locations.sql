/*- Queries for the locations table ------------------------------ */

-- name: GetSourceTypeByName :one
SELECT
    source_type_id,
    source_type_name
FROM loc.source_types
WHERE source_type_name = UPPER(sqlc.arg(source_type_name)::text);

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
) RETURNING l.location_id, l.location_name;

-- name: ListLocationIdsByType :many
SELECT
    l.location_id,
    l.location_name
FROM loc.locations AS l
INNER JOIN loc.location_types AS lt USING (location_type_id)
WHERE
    lt.location_type_name = UPPER(sqlc.arg(location_type_name)::text)
ORDER BY l.location_id;

-- name: ListLocationsByType :many
SELECT *
FROM loc.locations AS l
INNER JOIN loc.location_types AS lt USING (location_type_id)
WHERE
    lt.location_type_name = UPPER(sqlc.arg(location_type_name)::text)
ORDER BY l.location_id;

-- name: GetLocationById :one
SELECT
    l.location_id,
    l.location_name,
    ST_ASTEXT(l.geom)::text AS geom,
    lt.location_type_name,
    ST_Y(l.centroid)::real AS latitude,
    ST_X(l.centroid)::real AS longitude
FROM loc.locations AS l
INNER JOIN loc.location_types AS lt USING (location_type_id)
WHERE l.location_id = $1;

-- name: GetLocationGeoJSONByIds :one
/* GetLocationGeoJSONByIds returns a GeoJSON FeatureCollection for the given location IDs.
 * The input is an array of location IDs.
 * The simplification level can be adjusted via the `simplification_level` argument.
 */
SELECT
    JSON_BUILD_OBJECT(
        'type', 'FeatureCollection',
        'features', JSON_AGG(
            ST_ASGEOJSON(
                sl.*, id_column => 'location_id'::text, geom_column => 'geom_simple'
            )::jsonb
        )
    ) AS geojson
FROM (
    SELECT
        l.location_id,
        l.location_name,
        lt.location_type_name,
        ST_SIMPLIFYPRESERVETOPOLOGY(l.geom, sqlc.arg(simplification_level)::real) AS geom_simple
    FROM loc.locations AS l
    JOIN loc.location_types AS lt USING (location_type_id)
    WHERE l.location_id = ANY(sqlc.arg(location_ids)::int [])
) AS sl;

-- name: GetLocationIdsWithin :many
/* GetLocationIdsWithin returns all location IDs that are within the geometry of a given location.
 * The input is a location ID.
 */
SELECT
    l.location_id,
    l.location_name,
    ST_Y(l.centroid)::real AS latitude,
    ST_X(l.centroid)::real AS longitude
FROM loc.locations AS l
INNER JOIN
    loc.locations AS l_outer ON ST_WITHIN(
        l.geom,
        l_outer.geom
    )
WHERE l_outer.location_id = $1;

/*- Queries for the sources table -------------------------------------*/

-- name: GetSource :one
SELECT
    s.capacity,
    s.capacity_unit_prefix_factor,
    s.capacity_limit_sip,
    s.source_type_id,
    s.metadata::json AS metadata,
    l.location_name,
    ST_Y(l.centroid)::real AS latitude,
    ST_X(l.centroid)::real AS longitude
FROM loc.sources AS s
INNER JOIN loc.source_types AS st USING (source_type_id)
INNER JOIN loc.locations AS l USING (location_id)
WHERE
    s.location_id = $1
    AND st.source_type_name = UPPER(sqlc.arg(source_type_name)::text);

-- name: ListLocationsSources :many
SELECT
    s.location_id,
    l.location_name,
    ST_X(l.centroid)::real AS longitude,
    ST_Y(l.centroid)::real AS latitude,
    s.capacity,
    s.capacity_unit_prefix_factor,
    s.capacity_limit_sip,
    s.metadata
FROM loc.sources AS s
INNER JOIN loc.locations AS l USING (location_id)
WHERE
    s.location_id = ANY(sqlc.arg(location_ids)::integer [])
    AND s.source_type_id = $1;

-- name: CreateSource :one
INSERT INTO loc.sources (
    location_id, source_type_id, capacity, capacity_unit_prefix_factor, capacity_limit_sip, metadata
) SELECT
    $1,
    $2,
    $3,
    $4,
    sqlc.narg(capacity_limit_percent)::smallint,
    sqlc.narg(metadata)::json::jsonb
RETURNING source_id, capacity, capacity_unit_prefix_factor;

-- name: UpdateSource :exec
/* UpdateSource modifies an existing location source record.
 * Updates targeting tracked columns (capacity, capacity_unit_prefix_factor, capacity_limit).
 * Fields that want to remain unchanged should be set to their current values,
 * as the database cannot know if NULL is intended to be a new value or a flag to ignore the update.
 */
UPDATE loc.sources SET
    capacity = $3,
    capacity_unit_prefix_factor = $4,
    capacity_limit_sip = $5,
    metadata = $6
WHERE
    location_id = $1
    AND source_type_id = $2;

-- name: DecomissionSource :exec
DELETE FROM loc.sources
WHERE
    location_id = $1
    AND source_type_id = $2;

-- name: ListSourceHistory :many
/* ListSourceHistory shows all the historical records for a given location and source type. */
SELECT
    sh.capacity,
    sh.capacity_unit_prefix_factor,
    sh.capacity_limit_sip,
    LOWER(sh.sys_period) AS valid_from
FROM loc.sources_history AS sh
INNER JOIN loc.sources AS s USING (source_id)
WHERE s.location_id = $1 AND s.source_type_id = $2
ORDER BY LOWER(sh.sys_period) DESC;

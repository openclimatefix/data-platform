/*- Queries for the locations table ------------------------------ */

-- name: GetSourceTypeByName :one
SELECT 
    source_type_id, source_type_name
FROM loc.source_types
WHERE source_type_name = UPPER(sqlc.arg(source_type_name)::text);

-- name: CreateLocation :one
INSERT INTO loc.locations AS l (
    location_name, geom, location_type_id 
) VALUES (
    UPPER(sqlc.arg(location_name)::text),
    ST_GeomFromText(sqlc.arg(geom)::text, 4326), --Ensure in WSG84
    (SELECT location_type_id FROM loc.location_types AS lt WHERE lt.location_type_name = UPPER(sqlc.arg(location_type_name)::text))
) RETURNING l.location_id, l.location_name;

-- name: ListLocationIdsByType :many
SELECT
    location_id, location_name
FROM loc.locations AS l
WHERE
    l.location_type_id = (
        SELECT location_type_id
        FROM loc.location_types
        WHERE location_type_name = UPPER(sqlc.arg(location_type_name)::text)
    )
ORDER BY l.location_id;

-- name: ListLocationsByType :many
SELECT
    *
FROM loc.locations AS l
WHERE
    l.location_type_id = (
        SELECT location_type_id
        FROM loc.location_types
        WHERE location_type_name = UPPER(sqlc.arg(location_type_name)::text)
    )
ORDER BY l.location_id;

-- name: GetLocationById :one
SELECT 
    l.location_id,
    l.location_name,
    ST_AsText(l.geom)::text AS geom,
    (SELECT location_type_name FROM loc.location_types WHERE location_type_id = l.location_type_id) AS location_type_name,
    ST_Y(l.centroid)::real AS latitude,
    ST_X(l.centroid)::real AS longitude
FROM loc.locations AS l
WHERE l.location_id = $1;

-- name: GetLocationGeoJSONByIds :one
/* GetLocationGeoJSONByIds returns a GeoJSON FeatureCollection for the given location IDs.
 * The input is an array of location IDs.
 * The simplification level can be adjusted via the `simplification_level` argument.
 */
SELECT json_build_object(
    'type', 'FeatureCollection',
    'features', json_agg(
        ST_AsGeoJSON(sl.*, id_column => 'location_id'::text, geom_column => 'geom_simple')::jsonb
    )
) AS geojson
FROM (
    SELECT 
        l.location_id,
        l.location_name,
        (SELECT location_type_name FROM loc.location_types WHERE location_type_id = l.location_type_id) AS location_type_name,
        ST_SimplifyPreserveTopology(l.geom, sqlc.arg(simplification_level)::real) AS geom_simple
    FROM loc.locations AS l
    WHERE l.location_id = ANY(sqlc.arg(location_ids)::int[])
) AS sl;

-- name: GetLocationIdsWithin :many
/* GetLocationIdsWithin returns all location IDs that are within the geometry of a given location.
 * The input is a location ID.
 */
SELECT 
    l.location_id, 
    l.location_name
FROM loc.locations AS l
WHERE ST_Within(
    l.geom,
    (SELECT geom FROM loc.locations ll WHERE ll.location_id = $1)
);

/*- Queries for the sources table -------------------------------------*/

-- name: GetSource :one
SELECT
    ls.capacity,
    ls.capacity_unit_prefix_factor,
    ls.capacity_limit_sip,
    ls.source_type_id,
    ls.metadata::json AS metadata,
    l.location_name,
    ST_Y(l.centroid)::real AS latitude,
    ST_X(l.centroid)::real AS longitude
FROM loc.sources AS ls
JOIN loc.source_types AS st ON ls.source_type_id = st.source_type_id
JOIN loc.locations AS l USING (location_id)
WHERE
    ls.location_id = $1
    AND st.source_type_name = UPPER(sqlc.arg(source_type_name)::text);

-- name: ListLocationsSources :many
SELECT 
    location_id,
    capacity,
    capacity_unit_prefix_factor,
    capacity_limit_sip,
    metadata
FROM loc.sources
WHERE 
    location_id = ANY(sqlc.arg(location_ids)::integer[])
    AND source_type_id = $1;

-- name: CreateSource :one
INSERT INTO loc.sources (
    location_id, source_type_id, capacity, capacity_unit_prefix_factor, capacity_limit_sip, metadata
) SELECT 
    $1, $2, $3, $4,
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
    capacity,
    capacity_unit_prefix_factor,
    capacity_limit_sip,
    LOWER(sys_period) AS valid_from
FROM loc.sources_history
WHERE 
    source_id = (SELECT source_id FROM loc.sources WHERE location_id = $1 AND source_type_id = $2) 
    ORDER BY LOWER(sys_period) DESC;


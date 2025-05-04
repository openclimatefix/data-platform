/*- Queries for the locations table ------------------------------ */

-- name: CreateLocation :one
INSERT INTO loc.locations AS l (
    location_name, geom, location_type_id 
) VALUES (
    $2,
    ST_GEOMFROMTEXT(sqlc.arg(geom)::text, 4326), --Ensure in WSG84
    (SELECT location_type_id FROM loc.location_types AS lt WHERE lt.location_type_name = $1)
) RETURNING l.location_id;

-- name: ListLocationIdsByType :many
SELECT location_id, location_name FROM loc.locations AS l
WHERE l.location_type_id = (SELECT location_type_id FROM loc.location_types WHERE location_type_name = $1)
ORDER BY l.location_id;

-- name: ListLocationsByType :many
SELECT * FROM loc.locations AS l
WHERE l.location_type_id = (SELECT location_type_id FROM loc.location_types WHERE location_type_name = $1)
ORDER BY l.location_id;

-- name: ListLocationGeometryByType :many
SELECT location_name, ST_AsText(geom) FROM loc.locations AS l
WHERE l.location_type_id = (SELECT location_type_id FROM loc.location_types WHERE location_type_name = $1);

-- name: GetLocationById :one
SELECT * FROM loc.locations
WHERE location_id = $1;


/*- Queries for the location_sources table --------------------------- */
-- Get latest active record via the UPPER(sys_period) IS NULL condition

-- name: GetLocationSourceByType :one
SELECT (
    record_id, capacity, capacity_unit_prefix_factor, metadata
) FROM loc.location_sources
WHERE 
    location_id = $1
    AND source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    AND UPPER(sys_period) IS NULL;

-- name: CreateLocationSource :one
INSERT INTO loc.location_sources (
    location_id, source_type_id, capacity,
    capacity_unit_prefix_factor, metadata
) VALUES (
    $1, (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2),
    $3, $4, $5
) RETURNING record_id;

-- name: UpdateLocationSourceCapacity :exec
UPDATE loc.location_sources SET
    capacity = $3,
    capacity_unit_prefix_factor = $4
WHERE 
    location_id = $1
    AND source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    AND UPPER(sys_period) IS NULL;

-- name: UpdateLocationSourceMetadata :exec
UPDATE loc.location_sources SET
    metadata = $3
WHERE 
    location_id = $1
    AND source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    AND UPPER(sys_period) IS NULL;
    
-- name: UpdateLocationSource :exec
UPDATE loc.location_sources SET
    capacity = $3,
    capacity_unit_prefix_factor = $4,
    metadata = $5
WHERE 
    location_id = $1
    AND source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    AND UPPER(sys_period) IS NULL;
    
-- name: DecomissionLocationSource :exec
DELETE FROM loc.location_sources
WHERE 
    location_id = $1
    AND source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    AND UPPER(sys_period) IS NULL;

-- name: ListLocationSourceHistoryByType :many
SELECT (
    record_id, capacity, capacity_unit_prefix_factor, metadata, sys_period
) FROM loc.location_sources
WHERE 
    location_id = $1
    AND source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    ORDER BY LOWER(sys_period) DESC;


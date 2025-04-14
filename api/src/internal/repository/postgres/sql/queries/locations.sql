/*- Queries for the locations table ------------------------------ */

-- name: CreateLocation :one
WITH location_type_id AS (
    SELECT location_type_id FROM loc.location_types AS lt
    WHERE lt.name = $1
)
INSERT INTO loc.locations AS l (
    name, geom, location_type_id 
) VALUES (
    $2, $3, location_type_id
) RETURNING l.location_id;

-- name: ListLocationIdsByType :many
WITH location_type_id AS (
    SELECT location_type_id FROM loc.location_types AS lt
    WHERE lt.name = $1
)
SELECT location_id, name FROM loc.locations AS l
WHERE l.location_type_id = location_type_id
ORDER BY l.location_id;

-- name: ListLocationsByType :many
WITH location_type_id AS (
    SELECT location_type_id FROM loc.location_types AS lt
    WHERE lt.name = $1
)
SELECT * FROM loc.locations AS l
WHERE l.location_type_id = location_type_id
ORDER BY l.location_id;

-- name: ListLocationGeometryByType :many
WITH location_type_id AS (
    SELECT location_type_id FROM loc.location_types AS lt
    WHERE lt.name = $1
)
SELECT name, ST_AsText(geom) FROM loc.locations AS l
WHERE l.location_type_id = location_type_id;

-- name: GetLocationById :one
SELECT * FROM loc.locations
WHERE location_id = $1;


/*- Queries for the location_sources table --------------------------- */

-- name: CreateLocationSource :one
WITH source_type_id AS (
    SELECT source_type_id FROM loc.source_types AS st
    WHERE st.name = $2
)
INSERT INTO loc.location_sources (
    location_id, source_type_id, capacity,
    capacity_unit_prefix_factor, metadata
) VALUES (
    $1, source_type_id, $3,
    $4, $5
) RETURNING record_id;

-- name: UpdateLocationSourceCapacity :exec
WITH source_type_id AS (
    SELECT source_type_id FROM loc.source_types AS st
    WHERE st.name = $2
)
UPDATE loc.location_sources SET
    capacity = $3,
    capacity_unit_prefix_factor = $4
WHERE 
    location_id = $1
    AND source_type_id = source_type_id
    AND UPPER(sys_period) IS NULL; -- Currently active record

-- name: UpdateLocationSourceMetadata :exec
WITH source_type_id AS (
    SELECT source_type_id FROM loc.source_types AS st
    WHERE st.name = $2
)
UPDATE loc.location_sources SET
    metadata = $3
WHERE 
    location_id = $1
    AND source_type_id = source_type_id
    AND UPPER(sys_period) IS NULL; -- Currently active record
    
-- name: UpdateLocationSource :exec
WITH source_type_id AS (
    SELECT source_type_id FROM loc.source_types AS st
    WHERE st.name = $2
)
UPDATE loc.location_sources SET
    capacity = $3,
    capacity_unit_prefix_factor = $4,
    metadata = $5
WHERE 
    location_id = $1
    AND source_type_id = source_type_id
    AND UPPER(sys_period) IS NULL; -- Currently active record
    
-- name: DecomissionLocationSource :exec
WITH source_type_id AS (
    SELECT source_type_id FROM loc.source_types AS st
    WHERE st.name = $2
)
DELETE FROM loc.location_sources
WHERE 
    location_id = $1
    AND source_type_id = source_type_id
    AND UPPER(sys_period) IS NULL; -- Currently active record

-- name: ListLocationSourceHistoryByType :many
WITH source_type_id AS (
    SELECT source_type_id FROM loc.source_types AS st
    WHERE st.name = $2
)
SELECT (
    record_id, capacity, capacity_unit_prefix_factor, metadata, sys_period
) FROM loc.location_sources
WHERE 
    location_id = $1
    AND source_type_id = source_type_id
    ORDER BY LOWER(sys_period) DESC;


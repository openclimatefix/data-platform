-- name: CreateLocationSite :one
WITH new_loc_id AS (
    INSERT INTO loc.locations (
        name, latitude, longitude, capacity, capicity_unit_prefix_factor
    ) VALUES (
        $1, $2, $3, $4, $5
    ) RETURNING location_id
)
INSERT INTO loc.site_metadata (
    location_id, client_name, client_site_id, yaw_degrees, pitch_degrees, energy_source
) VALUES (
    new_loc_id, $5, $6, $7, $8, $9
) RETURNING location_id;

-- name: CreateLocationRegion :one
WITH new_loc_id AS (
    INSERT INTO loc.locations (
        name, latitude, longitude, capacity, capacity_unit_prefix_factor
    ) VALUES (
        $1, $2, $3, $4, $5
    ) RETURNING location_id
)
INSERT INTO loc.region_metadata (
    location_id, region_name, boundary_geojson
) VALUES (
    new_loc_id, $6, $7
) RETURNING location_id;

-- name: ListLocations :many
SELECT * FROM loc.locations
ORDER BY location_id;

-- name: ListRegions :many
SELECT * FROM loc.locations as l
LEFT OUTER JOIN loc.region_metadata USING (location_id)
ORDER BY l.location_id;

-- name: ListSites :many
SELECT * FROM loc.locations as l
LEFT OUTER JOIN loc.site_metadata USING (location_id)
ORDER BY l.location_id;


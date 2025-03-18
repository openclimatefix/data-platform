-- name: CreateLocationSite :one
WITH 
    site_subtype AS (
        SELECT id FROM loc.location_subtypes WHERE subtype = 'site'
    ),
    new_loc_id AS (
        INSERT INTO loc.locations (
            name, latitude, longitude, capacity_kw, location_type
        ) VALUES (
            $1, $2, $3, $4, site_subtype
        ) RETURNING id
    )
INSERT INTO loc.site_metadata (
    location_id, client_name, client_site_id, yaw_degrees, pitch_degrees, energy_source
) VALUES (
    new_loc_id, $5, $6, $7, $8, $9
) RETURNING location_id, id AS site_id;
REFRESH MATERIALIZED VIEW loc.sites;

-- name: CreateLocationRegion :one
WITH
    region_subtype AS (
        SELECT id FROM loc.location_subtypes WHERE subtype = 'region'
    ),
    new_loc_id AS (
        INSERT INTO loc.locations (
            name, latitude, longitude, capacity_kw, location_type
        ) VALUES (
            $1, $2, $3, $4, region_subtype
        ) RETURNING id
    )
INSERT INTO loc.region_metadata (
    location_id, created_utc, region_name, boundary_geojson
) VALUES (
    new_loc_id, $5, $6, $7
) RETURNING location_id, id AS region_id;
REFRESH MATERIALIZED VIEW loc.regions;

-- name: GetLocationSites

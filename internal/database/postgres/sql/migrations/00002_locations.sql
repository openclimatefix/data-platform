-- +goose Up

/*
 * Schema and tables to handle location-based data.
 * 
 * The generation data we store, be it predicted or otherwise, is always tied to a certain 
 * location. These locations vary in size and scope, from a single site to an entire country,
 * and the metadata we may want to store about them will also vary accordingly.

 * From an application standpoint, the location is pertinent in the case where we care about
 * the generated power as a fraction of the capacity of the location, as well as allowing us
 * to represent the data on a map.
 */

CREATE SCHEMA loc;
CREATE EXTENSION IF NOT EXISTS "btree_gist";
CREATE EXTENSION IF NOT EXISTS "postgis";

/*- Lookups -----------------------------------------------------------------------------------*/

-- Lookup table to store different source types
CREATE TABLE loc.source_types(
    source_type_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    source_type_name TEXT NOT NULL
        CHECK (
            LENGTH(source_type_name) > 0
            AND LENGTH(source_type_name) <= 48
            AND source_type_name = UPPER(source_type_name)
        ),
    PRIMARY KEY (source_type_id),
    UNIQUE (source_type_name)
);
INSERT INTO loc.source_types (source_type_name) VALUES ('SOLAR'), ('WIND'), ('HYDRO'), ('BATTERY');

-- Lookup table to store different location types
CREATE TABLE loc.location_types(
    location_type_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    location_type_name TEXT NOT NULL
        CHECK (
            LENGTH(location_type_name) > 0
            AND LENGTH(location_type_name) <= 24
            AND location_type_name = UPPER(location_type_name)
        ),
    PRIMARY KEY (location_type_id),
    UNIQUE (location_type_name)
);
INSERT INTO loc.location_types (location_type_name) VALUES ('SITE'), ('GSP'), ('DNO'), ('NATION');


/*- Tables ----------------------------------------------------------------------------------*/

-- Table to store spatial data for locations
CREATE TABLE loc.locations (
    location_uuid UUID DEFAULT uuidv7() NOT NULL,
    location_name TEXT NOT NULL
        CHECK ( LENGTH(location_name) > 0 AND location_name = UPPER(location_name) ),
    geom GEOMETRY(GEOMETRY, 4326) NOT NULL
        CHECK (
            ST_GeometryType(geom) IN ('ST_Point', 'ST_Polygon', 'ST_MultiPolygon')
            AND ST_SRID(geom) = 4326
            AND ST_NDIMS(geom) = 2
            AND ST_ISVALID(geom)
            AND ST_XMin(geom) >= -180 AND ST_XMax(geom) <= 180
            AND ST_YMin(geom) >= -90 AND ST_YMax(geom) <= 90
        ),
    location_type_id SMALLINT NOT NULL
        REFERENCES loc.location_types(location_type_id)
        ON DELETE RESTRICT,
    centroid GEOMETRY(POINT, 4326) GENERATED ALWAYS AS (ST_Centroid(geom)) STORED,
    geom_hash TEXT GENERATED ALWAYS AS (MD5(ST_AsBinary(geom))) STORED,
    PRIMARY KEY (location_uuid),
    UNIQUE (location_name, geom_hash)
);
-- Required index for efficient spatial-based queries
CREATE INDEX ON loc.locations USING GIST (geom);
-- Index for efficiently fetching e.g. all POINT geometry locations
CREATE INDEX ON loc.locations (ST_GeometryType(geom));
-- Index for finding all locations of a certain type
CREATE INDEX ON loc.locations (location_type_id);

/*
 * Table to store the temporal generation capability of locations.
 * Each location can have multiple sources of generation (solar, wind, etc),
 * and each source can change over time. For speed of writing, this is handled
 * via a simple valid-from timestamp field.
 */
CREATE TABLE loc.sources_history (
    source_type_id SMALLINT NOT NULL
        REFERENCES loc.source_types(source_type_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    -- Capacity in factors of powers of 10 Watts
    capacity SMALLINT NOT NULL
        CHECK ( capacity >= 0 ),
    -- Factor defining power of 10 to multiply the capacity by to get Watts
    capacity_unit_prefix_factor SMALLINT DEFAULT (0) NOT NULL
        CHECK (
            capacity_unit_prefix_factor >= 0
            AND capacity_unit_prefix_factor % 3 = 0
            AND capacity_unit_prefix_factor <= 18 -- ExaWatts surely sufficient...
        ),
    -- Capacity cap, (for instance during curtailment or repair work),
    -- encoded as a smallint percentage (sip) of the capacity; with 0 representing 0%
    -- AND 30000 representing 100% of the capacity. However, since things are mostly
    -- not limited, NULL indicates no limit, so 30000 is an invalid value.
    capacity_limit_sip SMALLINT DEFAULT NULL
        CHECK (
            capacity_limit_sip IS NULL
            OR (capacity_limit_sip >= 0 AND capacity_limit_sip < 30000)
        ),
    valid_from_utc TIMESTAMP DEFAULT NOW() NOT NULL,
    location_uuid UUID NOT NULL
        REFERENCES loc.locations(location_uuid)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    -- Metadata about the source, e.g. tilt, orientation, etc.
    metadata JSONB DEFAULT NULL
        CHECK ( metadata IS NULL OR metadata <> '{}'::jsonb ), -- Null is cheaper
    PRIMARY KEY (location_uuid, source_type_id, valid_from_utc)
);

/*
 * Materialized view to store the state of sources over time with a system period.
 * This allows for quicker reads of the state of sources at a given time.
 */
CREATE MATERIALIZED VIEW loc.sources_mv AS
SELECT
    location_uuid,
    source_type_id,
    capacity,
    capacity_unit_prefix_factor,
    capacity_limit_sip,
    TSRANGE(
        valid_from_utc,
        LEAD(valid_from_utc, 1) OVER (
            PARTITION BY location_uuid, source_type_id
            ORDER BY valid_from_utc)
        ) AS sys_period,
    metadata
FROM loc.sources_history;
-- Prevent overlapping records. Required for concurrent refreshes.
CREATE UNIQUE INDEX ON loc.sources_mv (location_uuid, source_type_id, sys_period);
CREATE INDEX ON loc.sources_mv USING GIST (sys_period);

-- +goose Down
DROP SCHEMA loc CASCADE;


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
    location_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
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
    PRIMARY KEY (location_id),
    UNIQUE (location_name, geom_hash)
);
-- Required index for efficient spatial-based queries
CREATE INDEX ON loc.locations USING GIST (geom);
-- Index for efficiently fetching e.g. all POINT geometry locations
CREATE INDEX ON loc.locations (ST_GeometryType(geom));
-- Index for finding all locations of a certain type
CREATE INDEX ON loc.locations (location_type_id);

/*
 * Table to store the current generation capability of locations.
 * Each location can have multiple sources of generation (solar, wind, etc),
 * and each source can change over time. This is handled via a temporal range
 * for each record and a trigger to update records when the source is modified.
 */
CREATE TABLE loc.sources (
    source_type_id SMALLINT NOT NULL
        REFERENCES loc.source_types(source_type_id)
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
    -- not limited, NULL indicates no limit, and 30000 is an invalid value.
    capacity_limit_sip SMALLINT DEFAULT NULL
        CHECK (
            capacity_limit_sip IS NULL
            OR (capacity_limit_sip >= 0 AND capacity_limit_sip < 30000)
        ),
    source_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    source_version INTEGER DEFAULT(1) NOT NULL,
    location_id INTEGER NOT NULL
        REFERENCES loc.locations(location_id)
        ON DELETE CASCADE,
    -- Metadata about the source, e.g. tilt, orientation, etc.
    metadata JSONB DEFAULT NULL
        CHECK ( metadata IS NULL OR metadata <> '{}'::jsonb ), -- Null is cheaper
    -- Period over which the record is valid (used for history table)
    sys_period TSRANGE NOT NULL
        DEFAULT TSRANGE(NOW()::TIMESTAMP, NULL, '[)')
        CHECK ( sys_period <> 'empty'::tsrange ),
    -- Each location can only have one source of each type
    UNIQUE (location_id, source_type_id),
    PRIMARY KEY (source_id)
);

/* Table to store the historic generation capability of locations.
 * Only the rows in this table will have their history tracked when updates are made to the main
 * table, so columns are dropped that have no need for a historical log.
 */
CREATE TABLE loc.sources_history (
    LIKE loc.sources INCLUDING CONSTRAINTS INCLUDING DEFAULTS,
    PRIMARY KEY (source_id, source_version)
);
ALTER TABLE loc.sources_history
    DROP COLUMN location_id,
    DROP COLUMN metadata,
    DROP COLUMN source_type_id;

CREATE TRIGGER source_history_trigger
    BEFORE INSERT OR UPDATE OR DELETE ON loc.sources
    FOR EACH ROW EXECUTE PROCEDURE versioning(
        'sys_period', 'loc.sources_history',
        true, true, -- Mitigate conflicts, ignore unchanged
        true, false, -- Include latest value in history, disable migration mode
        true, 'source_version' -- Increment version
    );

-- +goose Down
DROP SCHEMA loc CASCADE;


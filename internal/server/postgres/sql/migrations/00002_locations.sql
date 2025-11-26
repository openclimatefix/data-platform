-- +goose Up

/*
 * Schema and tables to handle location data.
 *
 * The generation data we store, be it predicted or otherwise, is always tied to a certain
 * geometry. These geometries vary in size and scope, from a single site to an entire country,
 * and the metadata we may want to store about them will also vary accordingly.

 * From an application standpoint, the geometry is pertinent in the case where we care about the
 * generated power as a fraction of the capacity of the geometry, as well as allowing us to
 * represent the data on a map.

 * To this degree, what the external application may consider a "location", is represented here as
 * a combination of a geometry (the spatial data), and a source (the energy generation capability).
 * One geometry can have multiple sources, e.g. the UK nation geometry can have solar, wind, etc.
 */

CREATE SCHEMA loc;
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS postgis;

/*- Lookups -----------------------------------------------------------------------------------*/

-- Lookup table to store different source types
CREATE TABLE loc.source_types (
    source_type_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    source_type_name TEXT NOT NULL,
    CONSTRAINT source_type_name_format_check CHECK (
        LENGTH(source_type_name) > 0
        AND LENGTH(source_type_name) <= 48
        AND source_type_name = LOWER(source_type_name)
    ),
    PRIMARY KEY (source_type_id),
    UNIQUE (source_type_name)
);
-- The ordering of insertion here matches the .proto enum definitions. Change with caution!
INSERT INTO loc.source_types (source_type_name) VALUES ('solar'), ('wind'), ('hydro'), ('battery');

-- Lookup table to store different geometry types
CREATE TABLE loc.geometry_types (
    geometry_type_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    geometry_type_name TEXT NOT NULL,
    CONSTRAINT geometry_type_name_format_check CHECK (
        LENGTH(geometry_type_name) > 0
        AND LENGTH(geometry_type_name) <= 24
        AND geometry_type_name = LOWER(geometry_type_name)
    ),
    PRIMARY KEY (geometry_type_id),
    UNIQUE (geometry_type_name)
);
-- The ordering of insertion here matches the .proto enum definitions. Change with caution!
INSERT INTO loc.geometry_types (geometry_type_name) VALUES ('site'), ('gsp'), ('dno'), ('nation');


/*- Tables ----------------------------------------------------------------------------------*/

-- Table to store spatial data for geometries
CREATE TABLE loc.geometries (
    geometry_uuid UUID DEFAULT UUIDV7() NOT NULL,
    geometry_name TEXT NOT NULL,
    CONSTRAINT geometry_name_check CHECK (
        LENGTH(geometry_name) > 0
        AND geometry_name = LOWER(geometry_name)
    ),
    geom GEOMETRY (GEOMETRY, 4326) NOT NULL,
    CONSTRAINT geom_validity_check CHECK (
        ST_GEOMETRYTYPE(geom) IN ('ST_Point', 'ST_Polygon', 'ST_MultiPolygon')
        AND ST_SRID(geom) = 4326
        AND ST_NDIMS(geom) = 2
        AND ST_ISVALID(geom)
        AND ST_XMIN(geom) >= -180 AND ST_XMAX(geom) <= 180
        AND ST_YMIN(geom) >= -90 AND ST_YMAX(geom) <= 90
    ),
    geometry_type_id SMALLINT NOT NULL
    REFERENCES loc.geometry_types (geometry_type_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
    centroid GEOMETRY (POINT, 4326) GENERATED ALWAYS AS (ST_CENTROID(geom)) STORED,
    geom_hash TEXT GENERATED ALWAYS AS (MD5(ST_ASBINARY(geom))) STORED,
    metadata JSONB DEFAULT NULL,
    PRIMARY KEY (geometry_uuid),
    UNIQUE (geometry_name, geom_hash)
);
-- Required index for efficient spatial-based queries
CREATE INDEX ON loc.geometries USING gist (geom);
-- Index for efficiently fetching e.g. all POINT geometry geometries
CREATE INDEX ON loc.geometries (ST_GEOMETRYTYPE(geom));
-- Index for finding all geometries of a certain type
CREATE INDEX ON loc.geometries (geometry_type_id);

/*
 * Table to store the temporal generation capability of geometries.
 * Each geometry can have multiple sources of generation (solar, wind, etc),
 * and each source can change over time. For speed of writing, this is handled
 * via a simple valid-from timestamp field.
 */
CREATE TABLE loc.sources_history (
    source_type_id SMALLINT NOT NULL
    REFERENCES loc.source_types (source_type_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
    -- Capacity in factors of powers of 10 Watts
    capacity SMALLINT NOT NULL,
    CONSTRAINT capacity_nonnegative_check CHECK (capacity >= 0),
    -- Factor defining power of 10 to multiply the capacity by to get Watts
    capacity_unit_prefix_factor SMALLINT DEFAULT (0) NOT NULL,
    CONSTRAINT capacity_unit_prefix_factor_valid_siprefix_check CHECK (
        capacity_unit_prefix_factor >= 0
        AND capacity_unit_prefix_factor <= 18 -- ExaWatts surely sufficient...
    ),
    -- Capacity cap, (for instance during curtailment or repair work),
    -- encoded as a smallint percentage (sip) of the capacity; with 0 representing 0%
    -- AND 30000 representing 100% of the capacity. However, since things are mostly
    -- not limited, NULL indicates no limit, so 30000 is an invalid value.
    capacity_limit_sip SMALLINT DEFAULT NULL,
    CONSTRAINT capacity_limit_sip_vaildity_check CHECK (
        capacity_limit_sip IS NULL
        OR (capacity_limit_sip >= 0 AND capacity_limit_sip < 30000)
    ),
    valid_from_utc TIMESTAMP DEFAULT NOW() NOT NULL,
    geometry_uuid UUID NOT NULL
    REFERENCES loc.geometries (geometry_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    -- Metadata about the source, e.g. tilt, orientation, etc.
    metadata JSONB DEFAULT NULL,
    CONSTRAINT metadata_nonempty_check CHECK (
        metadata IS NULL OR metadata <> '{}'::JSONB -- Null is cheaper
    ),
    PRIMARY KEY (geometry_uuid, source_type_id, valid_from_utc)
);

/*
 * Materialized view to store the state of sources over time with a system period.
 * This allows for quicker reads of the state of sources at a given time.
 */
CREATE MATERIALIZED VIEW loc.sources_mv AS
SELECT
    sh.geometry_uuid,
    sh.source_type_id,
    sh.capacity,
    sh.capacity_unit_prefix_factor,
    sh.capacity_limit_sip,
    sh.metadata,
    g.geometry_name,
    ST_X(g.centroid)::REAL AS longitude,
    ST_Y(g.centroid)::REAL AS latitude,
    TSRANGE(
        sh.valid_from_utc,
        LEAD(sh.valid_from_utc, 1) OVER (
            PARTITION BY sh.geometry_uuid, sh.source_type_id
            ORDER BY sh.valid_from_utc
        )
    ) AS sys_period
FROM loc.sources_history AS sh
INNER JOIN loc.geometries AS g USING (geometry_uuid);
-- Prevent overlapping records. Required for concurrent refreshes.
CREATE UNIQUE INDEX ON loc.sources_mv (geometry_uuid, source_type_id, sys_period);
CREATE INDEX ON loc.sources_mv USING gist (sys_period);


-- +goose Down
DROP SCHEMA loc CASCADE;

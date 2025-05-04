-- +goose Up

/*
Schema and tables to handle location-based data.

The generation data we store, be it predicted or otherwise, is always tied to a certain 
location. These locations vary in size and scope, from a single site to an entire country,
and the metadata we may want to store about them will also vary accordingly.

From an application standpoint, the location is pertinent in the case where we care about
the generated power as a fraction of the capacity of the location, as well as allowing us
to represent the data on a map.
*/

CREATE SCHEMA loc;
CREATE EXTENSION IF NOT EXISTS "btree_gist";
CREATE EXTENSION IF NOT EXISTS "postgis";

/*- Lookups -----------------------------------------------------------------------------------*/

-- Lookup table to store different source types
CREATE TABLE loc.source_types(
    source_type_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    source_type_name TEXT NOT NULL
        CHECK ( LENGTH(source_type_name) <= 24 AND source_type_name = LOWER(source_type_name) ),
    PRIMARY KEY (source_type_id),
    UNIQUE (source_type_name)
);
INSERT INTO loc.source_types (source_type_name) VALUES ('solar'), ('wind');

-- Lookup table to store different location types
CREATE TABLE loc.location_types(
    location_type_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    location_type_name TEXT NOT NULL
        CHECK ( LENGTH(location_type_name) <= 24 AND location_type_name = LOWER(location_type_name) ),
    PRIMARY KEY (location_type_id),
    UNIQUE (location_type_name)
);
INSERT INTO loc.location_types (location_type_name) VALUES ('site'), ('gsp'), ('dno'), ('nation');


/*- Tables ----------------------------------------------------------------------------------*/

-- Table to store spatial data for locations
CREATE TABLE loc.locations (
    location_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    location_name TEXT NOT NULL,
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
    PRIMARY KEY (location_id),
    UNIQUE (location_name, geom)
);
-- Required index for efficient spatial-based queries
CREATE INDEX ON loc.locations USING GIST (geom);
-- Index for efficiently fetching e.g. all POINT geometry locations
CREATE INDEX ON loc.locations (ST_GeometryType(geom));
-- Index for finding all locations of a certain type
CREATE INDEX ON loc.locations (location_type_id);

-- Table to store the current and historic generation capability of locations
CREATE TABLE loc.location_sources (
    record_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    location_id INTEGER NOT NULL
        REFERENCES loc.locations(location_id)
        ON DELETE CASCADE,
    source_type_id SMALLINT NOT NULL
        REFERENCES loc.source_types(source_type_id)
        ON DELETE RESTRICT,
    -- Capacity in factors of Watts
    capacity SMALLINT NOT NULL
        CHECK ( capacity >= 0 ),
    -- Factor defining power of 10 to multiply the capacity by
    capacity_unit_prefix_factor SMALLINT DEFAULT (0) NOT NULL
        CHECK ( capacity_unit_prefix_factor IN (0, 3, 6, 9, 12, 15) ),
    -- Capacity cap, measured in percent of the capacity (e.g. curtailment)
    capacity_limit SMALLINT
        CHECK ( capacity_limit IS NULL OR (capacity_limit >= 0 AND capacity_limit < 100) ),
    metadata JSONB
        CHECK ( metadata IS NULL OR metadata <> '{}'::jsonb ),
    sys_period TSRANGE NOT NULL
        DEFAULT TSRANGE(NOW()::TIMESTAMP, NULL, '[)')
        CHECK ( sys_period <> 'empty'::tsrange ),
    PRIMARY KEY (record_id),
    -- Prevent overlapping records for the same location and energy type
    EXCLUDE USING GIST (
        location_id WITH =,
        source_type_id WITH =,
        sys_period WITH &&
    )
);
-- Index to support exclusion constraint and lookups by location, type, and time
CREATE INDEX ON loc.location_sources USING GIST (location_id, source_type_id, sys_period);
-- Index for looking up a specific location generator's history
CREATE INDEX ON loc.location_sources (location_id, source_type_id);
-- Index for purely time-based queries across all records
CREATE INDEX ON loc.location_sources USING GIST (sys_period);


/*- Triggers --------------------------------------------------------------------------------*/

-- +goose StatementBegin
-- Function to ensure a properly kept historic record
CREATE OR REPLACE FUNCTION handle_location_source_change()
RETURNS TRIGGER AS $$
DECLARE current_ts TIMESTAMP := transaction_timestamp();
BEGIN
    -- Handle UPDATE operations representing source improvements
    IF (TG_OP = 'UPDATE') THEN
        -- Check if the capacity, capacity_unit_prefix_factor, or metadata have changed
        -- without an explicit change to the sys_period
        IF (
            OLD.capacity IS DISTINCT FROM NEW.capacity OR
            OLD.capacity_unit_prefix_factor IS DISTINCT FROM NEW.capacity_unit_prefix_factor OR
            OLD.metadata IS DISTINCT FROM NEW.metadata OR
            OLD.capacity_limit IS DISTINCT FROM NEW.capacity_limit
        ) THEN
            -- Close the validity period of the old record to current_ts (exclusive end)
            UPDATE loc.location_sources
            SET sys_period = TSRANGE(LOWER(OLD.sys_period), current_ts, '[)')
            WHERE record_id = OLD.record_id;
            -- Insert a new record with the updated values
            NEW.sys_period = TSRANGE(current_ts, NULL, '[]');
            INSERT INTO loc.location_sources (
                location_id, source_type_id, capacity, capacity_unit_prefix_factor,
                capacity_limit, metadata, sys_period
            ) VALUES (
                NEW.location_id, NEW.source_type_id, NEW.capacity, NEW.capacity_unit_prefix_factor,
                NEW.capacity_limit, NEW.metadata, NEW.sys_period
            );
            -- Cancel the original update action
            RETURN NULL;
        ELSE
            -- If capacity, capacity_unit_prefix_factor, and metadata are unchanged,
            -- prevent creating unecessary history
            RAISE WARNING '[INFO] Update on location_sources record_id % did not change
            tracked data (capacity, capacity_unit_prefix_factor, metadata): ignoring.',
            OLD.record_id;
            RETURN NULL;
        END IF;
    -- Handle DELETE operations representing decommissioning of sources
    ELSIF (TG_OP = 'DELETE') THEN
        -- Close the validity period of the old record to current_ts (exclusive end)
        UPDATE loc.location_sources
        SET sys_period = TSRANGE(LOWER(OLD.sys_period), current_ts, '[)')
        WHERE record_id = OLD.record_id;
        -- Cancel the original delete action
        RETURN NULL;
    END IF;
    -- Handle INSERT operations representing new sources
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_location_source_change
BEFORE UPDATE OR DELETE OR INSERT ON loc.location_sources
FOR EACH ROW EXECUTE FUNCTION handle_location_source_change();

-- +goose StatementEnd

-- +goose Down
DROP SCHEMA loc CASCADE;


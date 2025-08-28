-- +goose Up

/*
 * Schema and tables to handle observed generation data.
 *
 * Observations of generation data is usually measured by providers of inverters, which are
 * required in many sources of renewable energy to convert power from DC to AC. Partnerships
 * with these providers provide access to the data in order to test the accuracy of predictions.
*/

CREATE SCHEMA obs;

/*- Tables ----------------------------------------------------------------------------------*/

/* 
 * Table to store observers.
 * These are providers of actual recorded generation values from inverters
 * (mostly - looking at you, pvlive...)
*/
CREATE TABLE obs.observers (
    observer_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    observer_name TEXT NOT NULL
        CHECK (
            LENGTH(observer_name) > 0 AND LENGTH(observer_name) < 128
            AND observer_name = LOWER(observer_name)
        ),
    PRIMARY KEY (observer_id),
    UNIQUE (observer_name)
);

-- Table to store observed generation values
CREATE TABLE obs.observed_generation_values (
    -- The generation value as a percentage of the location capacity
    -- represented by a smallint percent (sip).
    -- Since it isn't impossible to measure a little over capacity,
    -- 30000 represents 100% of capacity instead of the max smallint value (32767).
    -- This allows for some measurement leeway.
    value_sip SMALLINT NOT NULL
        CHECK ( value_sip >= 0 ),
    source_type_id SMALLINT NOT NULL
        REFERENCES loc.source_types(source_type_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    observer_id INTEGER NOT NULL
        REFERENCES obs.observers(observer_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    observation_timestamp_utc TIMESTAMP NOT NULL
        CHECK ( observation_timestamp_utc <= CURRENT_TIMESTAMP + make_interval(days => 31) ),
    location_uuid UUID NOT NULL
        REFERENCES loc.locations(location_uuid)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    PRIMARY KEY (location_uuid, source_type_id, observer_id, observation_timestamp_utc)
)
-- Native partitioning. Note that unique indexes will only work if they include
-- the partition key.
PARTITION BY RANGE (observation_timestamp_utc);

-- Manage partitions with pg_partman
SELECT partman.create_parent(
    p_parent_table => 'obs.observed_generation_values',
    p_control => 'observation_timestamp_utc',
    p_type => 'range',
    p_interval => '1 week',
    p_automatic_maintenance => 'on',
    p_jobmon => false,
    p_premake => 7
);
UPDATE partman.part_config
SET retention = '1 month',
    -- Detacth as opposed to dropping partitions
    retention_keep_table = true,
    retention_keep_index = false,
    -- Retain the detatched partitions so they can be processed
    infinite_time_partitions = true
WHERE parent_table = 'obs.observed_generation_values';

-- +goose Down
DROP SCHEMA obs CASCADE;

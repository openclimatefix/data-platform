-- +goose Up

/*
Schema and tables to handle predicted generation data.

Predicted generation data is produced by various forecast models specific to a location.
A forecast is a set of predicted generations, beginning at the
*initialisation time*. Each subsequent generation's *target time* is equivalent to the
initialisation time plus the *horizon*.

From a frontend standpoint, the latest produced forecast is the most accurate
for a given location.
*/

CREATE SCHEMA pred;

/*- Tables ----------------------------------------------------------------------------------*/

/*
A forecast model is an ML model that generated predicted generation values.
Each model's name and version number uniquely identifies it.
*/
CREATE TABLE pred.models (
    model_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    name TEXT NOT NULL
        CHECK ( LENGTH(name) > 0 and LENGTH(name) < 64 ),
    version TEXT NOT NULL
        CHECK ( LENGTH(version) > 0 and LENGTH(version) < 64 ),
    created_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (model_id),
    UNIQUE (name, version)
);

/*
Forecasts refer to the generation predictions created by a specific version
of a forecast model for a specific location with a specific initialization time.
There can only be one forecast per location per initialization time per model,
reruns should replace old values.
*/
CREATE TABLE pred.forecasts (
    -- Type of energy source
    source_type_id SMALLINT NOT NULL
        REFERENCES loc.source_types(source_type_id)
        ON DELETE RESTRICT,
    forecast_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    location_id INTEGER NOT NULL
        REFERENCES loc.locations(location_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    model_id INTEGER NOT NULL
        REFERENCES pred.models(model_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    init_time_utc TIMESTAMP NOT NULL,
    PRIMARY KEY (forecast_id),
    UNIQUE (location_id, init_time_utc, source_type_id, model_id)
);

-- Index for efficiently finding a location's forecasts
CREATE INDEX ON pred.forecasts (location_id, init_time_utc);

/*
Table to store predicted generation values.
Predicted generation values are the output of a forecast model.
There can only be one predicted generation per forecast per horizon.
This table gets very large very quickly, so to save space,
data is stored as smallints where possible, and the columns are
ordered to allow for efficient bit-packing.
*/
CREATE TABLE pred.predicted_generation_values (
    -- Could have the init_time_utc here to denormalise, but it is encoded in
    -- the horizon value anyway, which is itself a more useful index 
    horizon_mins SMALLINT NOT NULL
        CHECK (horizon_mins >= 0),
    -- Predicted generation confidence level values, as a percentage of capacity
    -- represented by the smallint range. Since it isn't impossible to predict a little
    -- over capacity, 30000 represents 100% of capacity intead of the max smallint value
    -- (32767). This is to allow for a little bit of leeway in the predictions.
    p10 SMALLINT
        CHECK (p10 IS NULL or p10 >= 0),
    p50 SMALLINT NOT NULL
        CHECK (p50 >= 0),
    p90 SMALLINT
        CHECK (p90 IS NULL or p90 >= 0),
    forecast_id INTEGER NOT NULL
        REFERENCES pred.forecasts(forecast_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    -- Denormalisation from the location table to avoid joins
    location_id INTEGER NOT NULL
        REFERENCES loc.locations(location_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    -- Time that the predicted generation value corresponds to
    target_time_utc TIMESTAMP NOT NULL,
    metadata JSONB
        CHECK (metadata IS NULL or metadata != '{}'),
    PRIMARY KEY (target_time_utc, horizon_mins, forecast_id)
)
-- Native partitioning. Note that unique indexes will only work if they include
-- the partition key.
PARTITION BY RANGE (target_time_utc);

-- Index for cross section queries (one target time, many locations)
CREATE INDEX ON pred.predicted_generation_values (target_time_utc, horizon_mins);
-- Index for timeseries queries (one location, many target times)
CREATE INDEX ON pred.predicted_generation_values (location_id, target_time_utc, horizon_mins);
-- Index for getting specific forecast values
CREATE INDEX ON pred.predicted_generation_values (forecast_id, target_time_utc, horizon_mins);


-- Manage partitions with pg_partman
SELECT partman.create_parent(
    p_parent_table => 'pred.predicted_generation_values',
    p_control => 'target_time_utc',
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
WHERE parent_table = 'public.predicted_generation_values';

-- +goose Down
DROP SCHEMA pred CASCADE;


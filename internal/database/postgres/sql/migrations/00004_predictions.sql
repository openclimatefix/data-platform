-- +goose Up

/*
 * Schema and tables to handle predicted generation data.
 *
 * Predicted of generation values are produced by various forecast models for a specific location.
 * A forecast is a set of predicted generation values, beginning at the initialisation time. Each
 * subsequent generation's target time is equivalent to the initialisation time plus the horizon.
 *
 * The forecast produced most recently will likely be the most accurate.
 */

CREATE SCHEMA pred;

/*- Tables ----------------------------------------------------------------------------------*/

/*
 * A predictor is a source that generates predicted generation values. This is usually an ML model,
 * but could also be an analytical process. Each predictor's name and version number uniquely
 * identifies it.
 */
CREATE TABLE pred.predictors (
    predictor_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    predictor_name TEXT NOT NULL
        CHECK (
            LENGTH(predictor_name) > 0 AND LENGTH(predictor_name) < 64
            AND predictor_name = LOWER(predictor_name)
        ),
    predictor_version TEXT NOT NULL
        CHECK (
            LENGTH(predictor_version) > 0 AND LENGTH(predictor_version) < 64
            AND predictor_version = LOWER(predictor_version)
        ),
    created_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        CHECK (created_at_utc <= CURRENT_TIMESTAMP),
    PRIMARY KEY (predictor_id),
    UNIQUE (predictor_name, predictor_version)
);

/*
 * Forecasts refer to the generation predictions created by a specific version of a predictor for a
 * specific location. A forecast is created at an initialization time, and contains a timeseries of
 * predicted generation values. There can only be one forecast per location per initialization time
 * per predictor; reruns should replace old values.
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
    predictor_id INTEGER NOT NULL
        REFERENCES pred.predictors(predictor_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    init_time_utc TIMESTAMP NOT NULL
        CHECK (
            init_time_utc >= '2000-01-01 00:00:00'::timestamp
            AND init_time_utc < CURRENT_TIMESTAMP + make_interval(days => 30)
        ),
    PRIMARY KEY (forecast_id),
    UNIQUE (location_id, source_type_id, predictor_id, init_time_utc)
);

/*
 * Table to store predicted generation values. Predicted generation values are the output of a
 * forecast model. There can only be one predicted generation per forecast per horizon. This table
 * gets very large very quickly, so to save space, data is stored as smallints where possible, and
 * the columns are ordered to allow for efficient bit-packing.
 */
CREATE TABLE pred.predicted_generation_values (
    -- Could have the init_time_utc here to denormalise, but it is encoded in
    -- the horizon value anyway, which is itself a more useful index 
    horizon_mins SMALLINT NOT NULL
        CHECK (horizon_mins >= 0),
    -- Predicted generation confidence level values, as a percentage of capacity
    -- represented by a smallint percentage (sip). Since it isn't impossible to
    -- predict a little over capacity, 30000 represents 100% of capacity
    -- intead of the max smallint value (32767). This is to allow for a
    -- little bit of leeway in the predictions.
    p50_sip SMALLINT NOT NULL
        CHECK (p50_sip >= 0),
    p10_sip SMALLINT DEFAULT NULL
        CHECK (p10_sip IS NULL or p10_sip >= 0),
    p90_sip SMALLINT DEFAULT NULL
        CHECK (p90_sip IS NULL or p90_sip >= 0),
    forecast_id INTEGER NOT NULL
        REFERENCES pred.forecasts(forecast_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    -- Time that the predicted generation value corresponds to
    target_time_utc TIMESTAMP NOT NULL,
    metadata JSONB DEFAULT NULL
        CHECK (metadata IS NULL OR metadata != '{}'),
    PRIMARY KEY (forecast_id, target_time_utc, horizon_mins)
)
-- Native partitioning. Note that unique indexes will only work if they include
-- the partition key.
PARTITION BY RANGE (target_time_utc);

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


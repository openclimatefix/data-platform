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
 * A forecaster is a source that generates forecast values. This is usually an ML model,
 * but could also be an analytical process. Each forecaster's name and version number uniquely
 * identifies it.
 */
CREATE TABLE pred.forecasters (
    forecaster_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    forecaster_name TEXT NOT NULL,
    CONSTRAINT forecaster_name_format_check CHECK (
        LENGTH(forecaster_name) > 0 AND LENGTH(forecaster_name) < 64
        AND forecaster_name = LOWER(forecaster_name)
    ),
    forecaster_version TEXT NOT NULL,
    CONSTRAINT forecaster_version_format_check CHECK (
        LENGTH(forecaster_version) > 0 AND LENGTH(forecaster_version) < 64
        AND forecaster_version = LOWER(forecaster_version)
    ),
    created_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT created_at_utc_nonfuture_check CHECK (created_at_utc <= CURRENT_TIMESTAMP),
    PRIMARY KEY (forecaster_id),
    UNIQUE (forecaster_name, forecaster_version)
);

/*
 * Forecasts refer to the set of forecast values, created by a specific version of a forecaster,
 * for a specific location, with some initialization time. Each forecast contains a timeseries of
 * forecast values. There can only be one forecast per location per initialization time per
 * forecaster; reruns should replace old values.
 */
CREATE TABLE pred.forecasts (
    -- Type of energy source
    source_type_id SMALLINT NOT NULL
        REFERENCES loc.source_types(source_type_id)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,
    value_resolution_mins SMALLINT NOT NULL,
    CONSTRAINT value_resolution_mins_size_check CHECK (
        value_resolution_mins > 0 AND value_resolution_mins <= 60
    ),
    init_time_utc TIMESTAMP NOT NULL,
    CONSTRAINT init_time_utc_recency_check CHECK (
        init_time_utc >= '2000-01-01 00:00:00'::timestamp
        AND init_time_utc < CURRENT_TIMESTAMP + make_interval(days => 30)
    ),
    forecaster_id INTEGER NOT NULL
        REFERENCES pred.forecasters(forecaster_id)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    location_uuid UUID NOT NULL
        REFERENCES loc.locations(location_uuid)
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    forecast_uuid UUID DEFAULT uuidv7() NOT NULL,
    PRIMARY KEY (forecast_uuid),
    UNIQUE (location_uuid, source_type_id, forecaster_id, init_time_utc)
);

/*
 * Table to store predicted generation values.
 * Predicted generation values are the output of a forecast model. There can only be one predicted
 * generation per forecast per horizon. This table gets very large very quickly, so to save space,
 * data is stored as smallints where possible, and the columns are ordered to allow for efficient
 * bit-packing.
 *
 * The pXX columns are for predicted generation confidence level values, as a percentage of
 * capacity represented by a smallint percentage (sip). Since it isn't impossible to predict a
 * little over capacity, 30000 represents 100% of capacity intead of the max smallint value (32767).
 * This is to allow for a little bit of leeway in the predictions.
 *
 * The horizon_mins column stores the number of minutes difference between the target_time_utc and
 * the initialization time of the forecast. It is a more useful index for the kinds of query we
 * care about, and enables determination of the init_time anyway.
 * The table has native partitioning that can then be managed by pg_partman. Note that unique
 * indexes will only work if they include the partition key.
 */
CREATE TABLE pred.predicted_generation_values (
    horizon_mins SMALLINT NOT NULL,
    CONSTRAINT horizon_mins_nonnegative_check CHECK (horizon_mins >= 0),
    p50_sip SMALLINT NOT NULL,
    CONSTRAINT p50_sip_nonnegative_check CHECK (p50_sip >= 0),
    p10_sip SMALLINT DEFAULT NULL,
    CONSTRAINT p10_sip_nonnegative_check CHECK (p10_sip IS NULL or p10_sip >= 0),
    p90_sip SMALLINT DEFAULT NULL
        CHECK (p90_sip IS NULL or p90_sip >= 0),
    target_time_utc TIMESTAMP NOT NULL,
    metadata JSONB DEFAULT NULL
        CHECK (metadata IS NULL OR metadata != '{}'),
    forecast_uuid UUID NOT NULL
        REFERENCES pred.forecasts(forecast_uuid)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    PRIMARY KEY (forecast_uuid, target_time_utc, horizon_mins)
)
PARTITION BY RANGE (target_time_utc);

/*
 * Manage partitions with pg_partman.
 * Highlights:
 * - `retention_keep_table = true`: detach old partitions instead of dropping them
 * - `infinite_time_partitions = true`: retain detached partitions indefinitely for processing
 */
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
    retention_keep_table = true,
    retention_keep_index = false,
    infinite_time_partitions = true
WHERE parent_table = 'pred.predicted_generation_values';

-- +goose Down
DROP SCHEMA pred CASCADE;


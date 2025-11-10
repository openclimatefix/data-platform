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

/*- Functions -------------------------------------------------------------------------------*/

/*
 * Check that all present values in a JSONB blob are valid forecaster statistic fractions.
 * Valid statistic fractions are defined as numeric values between 0 and 1.1 (inclusive),
 * with up to 4 digits for precision (this final constraint ensures that only 2 bytes are
 * used to store each value).
 */
-- +goose StatementBegin
CREATE FUNCTION pred.check_all_jsonb_values_are_valid_stat_fractions(stats_blob jsonb)
RETURNS boolean AS $$
DECLARE
    rec record;
    val_num numeric;
BEGIN
    IF stats_blob IS NULL THEN
        RETURN true;
    END IF;

    FOR rec IN SELECT key, value FROM jsonb_each(stats_blob) LOOP
        IF LENGTH(rec.key) = 0 OR LENGTH(rec.key) > 64 THEN
            RETURN false;
        END IF;
        IF rec.key <> LOWER(rec.key) THEN
            RETURN false;
        END IF;
        
        IF jsonb_typeof(rec.value) <> 'number' THEN
            RETURN false;
        END IF;
        val_num := rec.value::numeric;
        IF (val_num < 0 OR val_num > 1.1) 
            OR (val_num >= 0 AND val_num < 1 AND scale(val_num) > 4)
            OR (val_num >= 1 AND val_num <= 1.1 AND scale(val_num) > 3)
        THEN
            RETURN false;
        END IF;

    END LOOP;
    RETURN true;

EXCEPTION
    WHEN others THEN
        RETURN false;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose StatementEnd

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
    source_type_id SMALLINT NOT NULL
    REFERENCES loc.source_types (source_type_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
    value_resolution_mins SMALLINT NOT NULL,
    CONSTRAINT value_resolution_mins_size_check CHECK (
        value_resolution_mins > 0 AND value_resolution_mins <= 60
    ),
    forecaster_id INTEGER NOT NULL
    REFERENCES pred.forecasters (forecaster_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    init_time_utc TIMESTAMP NOT NULL,
    CONSTRAINT init_time_utc_recency_check CHECK (
        init_time_utc >= '2000-01-01 00:00:00'::TIMESTAMP
        AND init_time_utc < CURRENT_TIMESTAMP + MAKE_INTERVAL(days => 30)
    ),
    geometry_uuid UUID NOT NULL
    REFERENCES loc.geometries (geometry_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    forecast_uuid UUID DEFAULT UUIDV7() NOT NULL,
    target_period TSRANGE NOT NULL,
    CONSTRAINT target_period_valid_check CHECK (
        UPPER(target_period) > LOWER(target_period)
    ),
    CONSTRAINT target_period_recency_check CHECK (
        LOWER(target_period) >= '2000-01-01 00:00:00'::TIMESTAMP
        AND UPPER(target_period) < CURRENT_TIMESTAMP + MAKE_INTERVAL(days => 30)
    ),
    PRIMARY KEY (forecast_uuid),
    UNIQUE (geometry_uuid, source_type_id, forecaster_id, init_time_utc)
);
CREATE INDEX ON pred.forecasts USING GIST (target_period);

/*
 * Table to store predicted generation values.
 * Predicted generation values are the output of a forecast model. There can only be one predicted
 * generation per forecast per horizon. This table gets very large very quickly, so to save space,
 * data is stored as smallints where possible, and the columns are ordered to allow for efficient
 * bit-packing.
 *
 * The p50 column is for the characteristic predicted generation confidence level value, recorded
 * as a percentage of capacity (represented by a smallint percentage, "sip". this allows for
 * efficient database calculation on the value for aggregation etc. Also, since it isn't impossible
 * to predict a little over capacity, 30000 represents 100% of capacity intead of the max smallint
 * value (32767).
 *
 * Any other forecaster outputs (such as other quantiles or averages) should be stored in the
 * "other_stats_fractions" JSONB column, as fractions of capacity. This allows for flexibility in
 * what other statistics are stored, without needing to modify the table structure.
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
    target_time_utc TIMESTAMP NOT NULL,
    forecast_uuid UUID NOT NULL
    REFERENCES pred.forecasts (forecast_uuid)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
    metadata JSONB DEFAULT NULL
    CONSTRAINT metadata_nullifempty CHECK (
        metadata IS NULL OR metadata != '{}'
    ),
    other_stats_fractions JSONB DEFAULT NULL,
    CONSTRAINT other_stats_nullifempty CHECK (
        other_stats_fractions IS NULL OR other_stats_fractions != '{}'
    ),
    CONSTRAINT other_stats_valid_fractions_check
        CHECK (pred.check_all_jsonb_values_are_valid_stat_fractions(other_stats_fractions)),
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
    p_jobmon => FALSE,
    p_premake => 7
);
UPDATE partman.part_config
SET
    retention = '1 month',
    retention_keep_table = TRUE,
    retention_keep_index = FALSE,
    infinite_time_partitions = TRUE
WHERE parent_table = 'pred.predicted_generation_values';
SELECT partman.run_maintenance('pred.predicted_generation_values');

-- +goose Down
DROP SCHEMA pred CASCADE;

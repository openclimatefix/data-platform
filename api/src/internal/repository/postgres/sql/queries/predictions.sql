/* --- Models --- */

-- name: CreateModel :one
INSERT INTO pred.models (model_name, model_version) VALUES (
    $1, $2
) RETURNING model_id;

-- name: GetModelById :one
SELECT
    model_id, model_name, model_version, created_at_utc
FROM pred.models
WHERE model_id = $1;

-- name: ListModels :many
SELECT
    model_id, model_name, model_version, created_at_utc
FROM pred.models
ORDER BY created_at_utc DESC;

-- name: GetLatestModelByName :one
SELECT
    model_id, model_name, model_version, created_at_utc
FROM pred.models
WHERE model_name = $1
ORDER BY created_at_utc DESC
LIMIT 1;

-- name: GetDefaultModel :one
SELECT
    model_id, model_name, model_version, created_at_utc
FROM pred.models
WHERE is_default = true
LIMIT 1;

-- name: SetDefaultModel :exec
UPDATE pred.models AS m SET
    is_default = c.new_is_default
FROM (VALUES
    ((SELECT model_id FROM pred.models WHERE is_default = true), NULL), (sqlc.arg(model_id)::integer, true)
) AS c(model_id, new_is_default)
WHERE m.model_id = c.model_id;

/* --- Forecasts --- */

-- name: CreateForecast :one
INSERT INTO pred.forecasts(
    source_type_id, location_id, model_id, init_time_utc
) VALUES (
    (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2), $1, $3, $4
) RETURNING forecast_id, init_time_utc, source_type_id, location_id, model_id;

-- name: CreateForecastsUsingBatch :batchone
-- CreateForecastsUsingBatch inserts a new forecasts as a batch process.
INSERT INTO pred.forecasts(
    source_type_id, location_id, model_id, init_time_utc
) VALUES (
    (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2), $1, $3, $4
) RETURNING *;

-- name: CreatePredictionsAsInt16UsingCopy :copyfrom
-- CreatePredictionsAsInt16UsingCopy inserts predicted generation values using
-- postgres COPY protocol, making it the fastest way to perform large inserts of predictions.
-- Input p-values are expected as 16-bit integers, with 0 representing 0%
-- and 30000 representing 100% of capacity.
INSERT INTO pred.predicted_generation_values (
    horizon_mins, p10, p50, p90, forecast_id, target_time_utc, metadata
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
);

-- name: CreatePredictionsAsPercentUsingBatch :batchexec
-- CreatePredictedYieldsAsPercentUsingBatch inserts predicted generation values as a batch process.
-- Input p-values are expected as a percentage of capacity. This is more readable but
-- slower than using the COPY protocol.
INSERT INTO pred.predicted_generation_values (
    horizon_mins, p10, p50, p90, forecast_id, target_time_utc, metadata
) VALUES (
    sqlc.arg(horizon_mins)::integer,
    encode_pct(sqlc.narg(p10_pct)::real),
    encode_pct(sqlc.arg(p50_pct)::real),
    encode_pct(sqlc.narg(p90_pct)::real),
    sqlc.arg(forecast_id)::integer,
    sqlc.arg(target_time_utc)::timestamp,
    sqlc.narg(metadata)::jsonb
);

-- name: GetLatestForecastAtHorizon :one
-- GetLatestForecastAtHorizon retrieves the latest forecast for a given location,
-- source type, and model. Only forecasts that are older than the specified horizon
-- are considered.
SELECT
    f.forecast_id,
    f.init_time_utc,
    f.source_type_id,
    f.location_id,
    f.model_id
FROM pred.forecasts f
WHERE f.location_id = $1
AND f.source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
AND f.model_id = $3
AND f.init_time_utc <= sqlc.arg(pivot_timestamp)::timestamp - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer)
ORDER BY f.init_time_utc DESC
LIMIT 1;

-- name: GetForecast :one
SELECT
    f.forecast_id,
    f.init_time_utc,
    f.source_type_id,
    f.location_id,
    f.model_id
FROM pred.forecasts f
WHERE f.location_id = $1
AND f.source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
AND f.model_id = $3
AND f.init_time_utc = $4;

-- name: GetForecastsTimeComponent :many
WITH desired_init_times AS (
    SELECT 
        (d.day::date + make_time(sqlq.arg(hour)::integer, sqlc.arg(minute)::integer, 0))::timestamp AS init_time_utc 
    FROM generate_series(
        NOW() - INTERVAL '7 days',
        NOW() - INTERVAL '1 day',
        INTERVAL '1 day'
    ) AS d(day)
    ORDER BY d.day ASC
)
SELECT
    f.forecast_id,
    f.init_time_utc,
    f.source_type_id,
    f.location_id,
    f.model_id
FROM pred.forecasts f
INNER JOIN desired_init_times dit ON f.init_time_utc = dit.init_time_utc
WHERE f.location_id = $1
AND f.source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
AND f.model_id = $3;

-- name: GetPredictionsAsPercentByForecastID :many
-- GetPredictionsAsPercentByForecastID retrieves predicted generation values as percentages of
-- capacity for a specific forecast ID. This is slower than returning the values directly,
-- so use where readability or understandability is more important than performance.
SELECT
    horizon_mins,
    decode_smallint(p10) AS p10_pct,
    decode_smallint(p50) AS p50_pct,
    decode_smallint(p90) AS p90_pct,
    target_time_utc,
    metadata
FROM pred.predicted_generation_values
WHERE forecast_id = $1;

-- name: GetPredictionsAsInt16ByForecastID :many
-- GetPredictionsAsInt16ByForecastID retrieves predicted generation values as 16-bit integers,
-- with 0 representing 0% and 30000 representing 100% of capacity.
SELECT
    horizon_mins,
    p10 AS p10_int16,
    p50 AS p50_int16,
    p90 AS p90_int16,
    target_time_utc,
    metadata
FROM pred.predicted_generation_values
WHERE forecast_id = $1;

-- name: GetPredictionsTimeseriesAsPercentAtHorizon :many
-- GetPredictionsTimeseriesAsPercentAtHorizon retrieves predicted generation values as a timeseries.
-- Multiple forecasts make up the timeseries, so overlapping predictions are filtered
-- according to the lowest allowable horizon. The timeseries window is 36 hours ago to now.
-- Yields are returned as percentages of capacity.
-- Has been measured to be 10 times slower than returning the values directly, so use in non-critical paths
-- where readability or understandability is more important than performance.
WITH relevant_forecasts AS (
    -- Get all the forecasts that fall within the time window for the given location, source, and model
    SELECT
        f.forecast_id
    FROM pred.forecasts f
    WHERE f.location_id = $1
    AND f.source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    AND f.model_id = $3
    AND f.init_time_utc >= sqlc.arg(pivot_timestamp)::timestamp - MAKE_INTERVAL(
        mins => sqlc.arg(horizon_mins)::integer, hours => 36
    )
),
filteredPredictions AS (
    -- Get all the predicted generation values for the relevant forecasts who's horizon is greater than
    -- or equal to the specified horizon_mins
    SELECT
        pv.horizon_mins,
        pv.p10,
        pv.p50,
        pv.p90,
        pv.target_time_utc,
        pv.metadata
    FROM pred.predicted_generation_values pv
    INNER JOIN relevant_forecasts rf USING (forecast_id)
    WHERE pv.target_time_utc >= sqlc.arg(pivot_timestamp)::timestamp - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer, hours => 36)
    AND pv.horizon_mins >= sqlc.arg(horizon_mins)::integer
),
rankedPredictions AS (
    -- Rank the predictions by horizon_mins for each target_time_utc
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY target_time_utc ORDER BY horizon_mins ASC) AS rn
    FROM filteredPredictions
)
SELECT
    -- For each target time, choose the value with the lowest available horizon
    rp.horizon_mins,
    decode_smallint(p10) AS p10_pct,
    decode_smallint(p50) AS p50_pct,
    decode_smallint(p90) AS p90_pct,
    rp.target_time_utc,
    rp.metadata
FROM rankedPredictions rp
WHERE rp.rn = 1
ORDER BY rp.target_time_utc ASC;


-- name: GetPredictionsAsPercentAtTimeAndHorizonForLocations :many
-- GetPredictionsAsPercentAtTimeAndHorizonForLocations retrieves predicted generation values as percentages
-- of capacity for a specific time and horizon. This is useful for comparing predictions across multiple locations.
WITH relevant_forecasts AS (
    SELECT
        f.forecast_id,
        f.location_id,
        f.init_time_utc,
        ROW_NUMBER() OVER (PARTITION BY f.location_id ORDER BY f.init_time_utc DESC) AS rn
    FROM pred.forecasts f
    WHERE f.location_id = ANY(sqlc.arg(location_ids)::integer[])
    AND f.source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $1)
    AND f.model_id = $2
    AND f.init_time_utc <= sqlc.arg(time)::timestamp - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer)
),
latest_relevant_forecasts AS (
    SELECT
        rf.forecast_id,
        rf.location_id,
        rf.init_time_utc
    FROM relevant_forecasts rf
    WHERE rf.rn = 1
)
SELECT
    rf.location_id,
    pgv.horizon_mins,
    decode_smallint(pgv.p10) AS p10_pct,
    decode_smallint(pgv.p50) AS p50_pct,
    decode_smallint(pgv.p90) AS p90_pct,
    pgv.target_time_utc,
    pgv.metadata
FROM pred.predicted_generation_values pgv
INNER JOIN latest_relevant_forecasts rf USING (forecast_id)
WHERE pgv.horizon_mins = sqlc.arg(horizon_mins)::integer;


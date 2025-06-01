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
    ((SELECT model_id FROM pred.models WHERE is_default = true), false), (sqlc.arg(model_id)::integer, true)
) AS c(model_id, new_is_default)
WHERE m.model_id = c.model_id;

/* --- Forecasts --- */

-- name: CreateForecast :one
INSERT INTO pred.forecasts(
    source_type_id, location_id, model_id, init_time_utc
) VALUES (
    (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2), $1, $3, $4
) RETURNING forecast_id;
    
-- name: CreatePredictedGenerationValues :copyfrom
INSERT INTO pred.predicted_generation_values (
    horizon_mins, p10, p50, p90, forecast_id, target_time_utc, metadata
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
);

-- name: GetLatestForecastForLocationAtHorizon :one
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
AND f.init_time_utc <= CURRENT_TIMESTAMP - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer)
ORDER BY f.init_time_utc DESC
LIMIT 1;

-- name: GetPredictedGenerationValuesForForecast :many
SELECT
    horizon_mins,
    p10,
    p50,
    p90,
    target_time_utc,
    metadata
FROM pred.predicted_generation_values
WHERE forecast_id = $1;

-- name: GetWindowedPredictedGenerationValuesAtHorizon :many
WITH relevant_forecasts AS (
    -- Get all the forecasts that fall within the time window for the given location, source, and model
    SELECT
        f.forecast_id
    FROM pred.forecasts f
    WHERE f.location_id = $1
    AND f.source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    AND f.model_id = $3
    AND f.init_time_utc >= CURRENT_TIMESTAMP - MAKE_INTERVAL(
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
    INNER JOIN relevant_forecasts rf ON pv.forecast_id = rf.forecast_id
    WHERE pv.target_time_utc >= CURRENT_TIMESTAMP - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer, hours => 36)
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
    rp.p10,
    rp.p50,
    rp.p90,
    rp.target_time_utc,
    rp.metadata
FROM rankedPredictions rp
WHERE rp.rn = 1
ORDER BY rp.target_time_utc ASC;


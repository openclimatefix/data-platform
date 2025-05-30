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

-- name: GetPredictedGenerationValuesForLocationAtHorizon :many
WITH relevant_forecasts AS (
    SELECT
        f.forecast_id
    FROM pred.forecasts f
    WHERE f.location_id = $1
    AND f.source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    AND f.model_id = $2
    AND f.init_time_utc BETWEEN 
        -- Default window is 36 hours backwards
        CURRENT_TIMESTAMP - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer, hours => 36)
        AND CURRENT_TIMESTAMP - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer)
)
SELECT
    pgv.horizon_mins,
    pgv.p10,
    pgv.p50,
    pgv.p90,
    pgv.target_time_utc,
    pgv.metadata
FROM pred.predicted_generation_values pgv
INNER JOIN relevant_forecasts ON pgv.forecast_id = relevant_forecasts.forecast_id
WHERE pgv.target_time_utc BETWEEN
    CURRENT_TIMESTAMP - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer, hours => 36)
    AND CURRENT_TIMESTAMP - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer)
AND pgv.horizon_mins = sqlc.arg(horizon_mins)::smallint;



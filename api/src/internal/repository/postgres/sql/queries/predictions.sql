-- name: CreateModel :one
INSERT INTO pred.models (name, version) VALUES (
    $1, $2
) RETURNING model_id;

-- name: CreateForecast :one
INSERT INTO pred.forecasts(
    source_type_id, location_id, model_id, init_time_utc
) VALUES (
    (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2), $1, $3, $4
) RETURNING forecast_id;
    
-- name: CreatePredictedGenerationValues :copyfrom
INSERT INTO pred.predicted_generation_values (
    horizon_mins, p10, p50, p90, forecast_id, location_id, target_time_utc, metadata
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
);

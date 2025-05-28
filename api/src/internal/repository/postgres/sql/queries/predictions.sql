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

-- name: GetMinHorizonPredictedGenerationValuesForLocation :many
WITH ranked_predictions AS (
    SELECT
        pgv.target_time_utc,
        pgv.p10,
        pgv.p50,
        pgv.p90,
        pgv.horizon_mins,
        pgv.forecast_id,
        ROW_NUMBER() OVER (PARTITION BY pgv.target_time_utc ORDER BY pgv.horizon_mins ASC) as rn
    FROM
        pred.predicted_generation_values pgv
    WHERE
        pgv.location_id = $1
        AND pgv.target_time_utc >= CURRENT_TIMESTAMP - make_interval(hours => 36)
)
SELECT
    rp.target_time_utc,
    rp.p10,
    rp.p50,
    rp.p90,
    rp.horizon_mins,
    rp.forecast_id
FROM ranked_predictions rp
WHERE rp.rn = 1
ORDER BY rp.target_time_utc ASC;

-- name: GetCustomHorizonPredictedGenerationValuesForLocation: many
-- Find the forecast that was made closest to NOW - N minutes: the pivot forecast
WITH pivot_forecast AS (
    SELECT 
        f.forecast_id,
        f.init_time_utc
    FROM pred.forecasts f
    WHERE f.location_id = $1
        AND f.init_time_utc <= CURRENT_TIMESTAMP - make_interval(minutes => sqlc.arg(horizon_mins)::integer)
    ORDER BY f.init_time_utc DESC
    LIMIT 1
),
-- Select data for the "historical" part: target_time_utc <= pivot_time and horizon_mins = N
historical_part_candidates AS (
    SELECT
        pgv.target_time_utc,
        pgv.p10,
        pgv.p50,
        pgv.p90,
        pgv.horizon_mins,
        pgv.forecast_id,
        -- If multiple forecasts provide data for the same target_time with the specified horizon N,
        -- pick the one from the forecast with the most recent init_time.
        -- Use forecast_id as a secondary tie-breaker for full determinism.
        ROW_NUMBER() OVER (PARTITION BY pgv.target_time_utc ORDER BY f.init_time_utc DESC, f.forecast_id DESC) as rn_hist
    FROM
        pred.predicted_generation_values pgv
    INNER JOIN pred.forecasts f USING forecast_id
    LEFT JOIN pivot_forecast pf ON true
    WHERE
        pgv.location_id = $1
        AND f.location_id = $1
        AND pgv.target_time_utc BETWEEN
            CURRENT_TIMESTAMP - make_interval(hours => 36) AND
            pf.init_time_utc + make_interval(minutes => sqlc.arg(horizon_mins)::integer)
        AND pgv.horizon_mins = sqlc.arg(horizon_mins)::integer
),
selected_historical_part AS (
    SELECT target_time_utc, p10, p50, p90, horizon_mins, forecast_id
    FROM historical_part_candidates
    WHERE rn_hist = 1
),
-- Data going forwards is taken from the pivot forecast
future_part AS (
    SELECT
        pgv.target_time_utc,
        pgv.p10,
        pgv.p50,
        pgv.p90,
        pgv.horizon_mins,
        pgv.forecast_id
    FROM
        pred.predicted_generation_values pgv
    LEFT JOIN pivot_forecast_id pf ON true
    WHERE
        pgv.location_id = $1
        AND pgv.target_time_utc >= pf.init_time_utc + make_interval(minutes => sqlc.arg(horizon_mins)::integer)
)
-- Combine the historical and future/recent parts
SELECT h.target_time_utc, h.p10, h.p50, h.p90, h.horizon_mins, h.forecast_id FROM selected_historical_part h
UNION ALL
SELECT fut.target_time_utc, fut.p10, fut.p50, fut.p90, fut.horizon_mins, fut.forecast_id FROM future_part fut
ORDER BY target_time_utc ASC;


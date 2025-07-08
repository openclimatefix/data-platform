/* --- Predictor ------------------------------------------------------------------------------ */

-- name: CreatePredictor :one
INSERT INTO pred.predictors (predictor_name, predictor_version) VALUES (
    $1, $2
) RETURNING predictor_id;

-- name: GetPredictorElseLatest :one
/* GetPredictor retrieves a predictor by its name and version.
 * If no version is provided (empty string), it defaults to the latest version
 * for the given predictor name.
*/
SELECT
    predictor_id, predictor_name, predictor_version, created_at_utc
FROM pred.predictors
WHERE 
    predictor_name = sqlc.arg(predictor_name)::text
    AND predictor_version = COALESCE(
        NULLIF(sqlc.arg(predictor_version)::text, ''),
        (
            SELECT p.predictor_version
            FROM pred.predictors p
            WHERE p.predictor_name = sqlc.arg(predictor_name)::text
            ORDER BY p.created_at_utc DESC
            LIMIT 1
        )
    );

-- name: ListPredictors :many
SELECT
    predictor_id, predictor_name, predictor_version, created_at_utc
FROM pred.predictors
ORDER BY created_at_utc DESC;

/* --- Forecasts ------------------------------------------------------------------------------ */

-- name: CreateForecast :one
INSERT INTO pred.forecasts(
    location_id, source_type_id, predictor_id, init_time_utc
) VALUES (
    $1, $2, $3, $4
) RETURNING forecast_id, init_time_utc, source_type_id, location_id, predictor_id;

-- name: CreateForecastsUsingCopy :batchone
/* CreateForecastsUsingBatch inserts a new forecasts as a batch process. */
INSERT INTO pred.forecasts(
    location_id, source_type_id, predictor_id, init_time_utc
) VALUES (
    $1, $2, $3, $4
) RETURNING *;

-- name: CreatePredictionsAsInt16UsingCopy :copyfrom
/* CreatePredictionsAsInt16UsingCopy inserts predicted generation values using
 * postgres COPY protocol, making it the fastest way to perform large inserts of predictions.
 * Input p-values are expected as smallint percentages (sip) of capacity,
 * with 0 representing 0% and 30000 representing 100% of capacity.
 */
INSERT INTO pred.predicted_generation_values (
    horizon_mins, p10_sip, p50_sip, p90_sip, forecast_id, target_time_utc, metadata
) VALUES (
    $1, $2, $3, $4, $5, $6, $7
);

-- name: GetLatestForecastAtHorizonSincePivot :one
/* GetLatestForecastAtHorizonSincePivot retrieves the latest forecast for a given location,
 * source type, and predictor. Only forecasts that are older than the pivot time
 * minus the specified horizon are considered.
 */
SELECT
    f.forecast_id,
    f.init_time_utc,
    f.source_type_id,
    f.location_id,
    f.predictor_id
FROM pred.forecasts f
WHERE f.location_id = $1
AND f.source_type_id = $2
AND f.predictor_id = $3
AND f.init_time_utc <= sqlc.arg(pivot_timestamp)::timestamp - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer)
ORDER BY f.init_time_utc DESC
LIMIT 1;

-- name: ListPredictionsForForecast :many
/* ListPredictionsForForecast retrieves predicted generation values
 * for a given forecast as smallint percentages (sip) of capacity;
 * with 0 representing 0% and 30000 representing 100% of capacity.
 */
SELECT
    horizon_mins,
    p10_sip,
    p50_sip,
    p90_sip,
    target_time_utc,
    metadata
FROM pred.predicted_generation_values
WHERE forecast_id = $1;

-- name: ListPredictionsForLocation :many
/* ListPredictionsForLocation retrieves predicted generation values as a timeseries.
 * Multiple overlapping forecasts can make up the timeseries, so predictions with the same target time
 * are filtered by lowest allowable horizon (i.e. predicted closest to their target time).
 * Predicted values are smallint percentages (sip) of capcity;
 * with 0 representing 0% and 30000 representing 100% of capacity.
 */
WITH relevant_forecasts AS (
    /* Get all the forecasts that fall within the time window for the given location, source, and predictor */
    SELECT
        f.forecast_id
    FROM pred.forecasts f
    WHERE f.location_id = $1
    AND f.source_type_id = $2
    AND f.predictor_id = $3
    AND f.init_time_utc BETWEEN
        sqlc.arg(start_timestamp)::timestamp - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer)
        AND sqlc.arg(end_timestamp)::timestamp
),
filteredPredictions AS (
    /* Get all the predicted generation values for the relevant forecasts who's horizon is greater than
     * or equal to the specified horizon_mins */
    SELECT
        pv.horizon_mins,
        pv.p10_sip,
        pv.p50_sip,
        pv.p90_sip,
        pv.target_time_utc,
        pv.metadata
    FROM pred.predicted_generation_values pv
    INNER JOIN relevant_forecasts rf USING (forecast_id)
    WHERE pv.target_time_utc BETWEEN
        sqlc.arg(start_timestamp)::timestamp - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::integer)
        AND sqlc.arg(end_timestamp)::timestamp
    AND pv.horizon_mins >= sqlc.arg(horizon_mins)::integer
),
rankedPredictions AS (
    /* Rank the predictions by horizon_mins for each target_time_utc */
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY target_time_utc ORDER BY horizon_mins ASC) AS rn
    FROM filteredPredictions
)
SELECT
    /* For each target time, choose the value with the lowest available horizon */
    rp.horizon_mins,
    p10_sip,
    p50_sip,
    p90_sip,
    rp.target_time_utc,
    rp.metadata
FROM rankedPredictions rp
WHERE rp.rn = 1
ORDER BY rp.target_time_utc ASC;


-- name: ListPredictionsAtTimeForLocations :many
/* ListPredictionsAtTimeForLocations retrieves predicted generation values as percentages
 * of capacity for a specific time and horizon.
 * This is useful for comparing predictions across multiple locations.
 * Predicted values are 16-bit integers, with 0 representing 0% and 30000 representing 100% of capacity.
 */
WITH relevant_forecasts AS (
    SELECT
        f.forecast_id,
        f.location_id,
        f.init_time_utc,
        ROW_NUMBER() OVER (PARTITION BY f.location_id ORDER BY f.init_time_utc DESC) AS rn
    FROM pred.forecasts f
    WHERE f.location_id = ANY(sqlc.arg(location_ids)::integer[])
    AND f.source_type_id = $1
    AND f.predictor_id = $2
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
    pgv.p10_sip,
    pgv.p50_sip,
    pgv.p90_sip,
    pgv.target_time_utc,
    pgv.metadata
FROM pred.predicted_generation_values pgv
INNER JOIN latest_relevant_forecasts rf USING (forecast_id)
WHERE pgv.horizon_mins = sqlc.arg(horizon_mins)::integer;

-- name: GetWeekAverageDeltasForLocations :many
/* GetWeekAverageDeltasForLocations retrieves the average deltas between predicted and observed generation values
 * for a given source type, predictor, and observer, across a week of forecasts made with the same init time.
 * The pivot timestamp is used to determine the week and init time of interest.
 * The results are grouped by location and horizon.
 */
WITH desired_init_times AS (
    SELECT 
        (d.day::date + sqlc.arg(pivot_timestamp)::timestamp::time)::timestamp AS init_time_utc 
    FROM generate_series(
        sqlc.arg(pivot_timestamp)::timestamp::date - INTERVAL '7 days',
        sqlc.arg(pivot_timestamp)::timestamp::date - INTERVAL '1 day',
        INTERVAL '1 day'
    ) AS d(day)
    ORDER BY d.day ASC
),
relevant_forecasts AS (
    SELECT 
        f.forecast_id,
        f.init_time_utc,
        f.source_type_id,
        f.location_id,
        f.predictor_id
    FROM pred.forecasts f
    INNER JOIN desired_init_times dit ON f.init_time_utc = dit.init_time_utc
    WHERE f.location_id = ANY(sqlc.arg(location_ids)::integer[])
    AND f.source_type_id = $1
    AND f.predictor_id = $2
),
predicted_values AS (
    SELECT
        rf.location_id,
        rf.forecast_id,
        rf.source_type_id,
        pgv.target_time_utc,
        pgv.horizon_mins,
        pgv.p50_sip
    FROM relevant_forecasts rf
    INNER JOIN pred.predicted_generation_values pgv USING (forecast_id)
),
deltas AS (
    SELECT
        pv.location_id,
        pv.source_type_id,
        pv.forecast_id,
        pv.target_time_utc,
        pv.horizon_mins,
        pv.p50_sip - ov.value_sip AS delta_sip
    FROM predicted_values pv
    LEFT JOIN obs.observed_generation_values ov USING (location_id, source_type_id)
    WHERE
        ov.observer_id = $3
        AND ov.observation_time_utc = pv.target_time_utc
)
SELECT
    d.location_id,
    d.horizon_mins,
    AVG(d.delta_sip) AS avg_delta_sip
FROM deltas d
GROUP BY d.location_id, d.horizon_mins
ORDER BY d.location_id, d.horizon_mins;

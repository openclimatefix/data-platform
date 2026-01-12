/* --- Forecaster ------------------------------------------------------------------------------ */

-- name: CreateForecaster :one
INSERT INTO pred.forecasters (forecaster_name, forecaster_version) VALUES (
    LOWER(sqlc.arg(forecaster_name)::TEXT), LOWER(sqlc.arg(forecaster_version)::TEXT)
) RETURNING forecaster_id, forecaster_name, forecaster_version;

-- name: GetForecasterElseLatest :one
/* GetForecaster retrieves a forecaster by its name and version.
 * If no version is provided (empty string), it defaults to the latest version
 * for the given forecaster name.
*/
WITH desired_version AS (
    SELECT
        COALESCE(NULLIF(sqlc.arg(forecaster_version)::TEXT, ''), (
            SELECT forecaster_version
            FROM pred.forecasters
            WHERE forecaster_name = LOWER(sqlc.arg(forecaster_name)::TEXT)
            ORDER BY created_at_utc DESC
            LIMIT 1
        )) AS forecaster_version
)
SELECT
    p.forecaster_id,
    p.forecaster_name,
    p.forecaster_version,
    p.created_at_utc
FROM pred.forecasters AS p
    INNER JOIN desired_version ON TRUE
WHERE p.forecaster_name = LOWER(sqlc.arg(forecaster_name)::TEXT)
    AND p.forecaster_version = desired_version.forecaster_version;

-- name: GetForecastersByFilters :many
/* GetForecastersByFilters retrieves forecasters according to a few filters.
 * This may well deprecate the above query...
*/
WITH ranked_forecasters AS (
    SELECT
        forecaster_id,
        forecaster_name,
        forecaster_version,
        created_at_utc,
        ROW_NUMBER() OVER (
            PARTITION BY forecaster_name
            ORDER BY created_at_utc DESC
        ) AS rn
    FROM pred.forecasters
)
SELECT
    forecaster_id,
    forecaster_name,
    forecaster_version,
    created_at_utc
FROM ranked_forecasters
WHERE (
    ARRAY_LENGTH(sqlc.arg(forecaster_names)::TEXT [], 1) IS NULL
    OR forecaster_name = ANY(sqlc.arg(forecaster_names)::TEXT [])
)
AND (
    NOT sqlc.arg(latest_version_only)::BOOLEAN OR rn = 1
)
ORDER BY forecaster_name ASC, created_at_utc DESC;

/* --- Forecasts ------------------------------------------------------------------------------ */

-- name: CreateForecast :one
INSERT INTO pred.forecasts (
    geometry_uuid, source_type_id, forecaster_id, init_time_utc, value_resolution_mins, target_period
) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    TSRANGE(
        $4::TIMESTAMP + MAKE_INTERVAL(mins => sqlc.arg(first_horizon_mins)::INTEGER),
        $4::TIMESTAMP + MAKE_INTERVAL(mins => sqlc.arg(last_horizon_mins)::INTEGER),
        '[]'
    )
) RETURNING
    forecast_uuid,
    init_time_utc,
    source_type_id,
    geometry_uuid,
    forecaster_id,
    target_period;

-- name: DeleteForecast :exec
DELETE FROM pred.forecasts
WHERE forecast_uuid = $1;

-- name: CreatePredictedValues :copyfrom
/* CreatePredictedValues inserts predicted generation values using
 * postgres COPY protocol, making it the fastest way to perform large inserts of predictions.
 * Input p-values are expected as smallint percentages (sip) of capacity,
 * with 0 representing 0% and 30000 representing 100% of capacity.
 */
INSERT INTO pred.predicted_generation_values (
    horizon_mins, p50_sip, forecast_uuid, target_time_utc, other_stats_fractions, metadata
) VALUES (
    $1, $2, $3, $4, $5, $6
);

-- name: GetLatestForecastsAtHorizonSincePivot :many
/* GetLatestForecastAtHorizonSincePivot retrieves the latest forecasts for a given location
 * and source type made by all forecasters. Only forecasts that are older than the pivot time
 * minus the specified horizon are considered.
 */
SELECT DISTINCT ON (fr.forecaster_name)
    f.forecast_uuid,
    f.init_time_utc,
    f.source_type_id,
    f.geometry_uuid,
    fr.forecaster_name,
    fr.forecaster_version,
    UUIDV7_EXTRACT_TIMESTAMP(f.forecast_uuid) AS created_at_utc
FROM pred.forecasts AS f
    INNER JOIN pred.forecasters AS fr USING (forecaster_id)
WHERE f.geometry_uuid = $1
    AND f.source_type_id = $2
    AND f.init_time_utc <= sqlc.arg(pivot_timestamp)::TIMESTAMP - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::INTEGER)
    AND f.target_period @> sqlc.arg(pivot_timestamp)::TIMESTAMP
ORDER BY
    fr.forecaster_name ASC,
    f.init_time_utc DESC;

-- name: ListForecasts :many
/* ListForecasts retrieves all the forecasts for a given location, source type, and forecaster
 * between the input times. It does not return forecast values.
 */
WITH desired_forecaster AS (
    SELECT
        forecaster_id,
        forecaster_name,
        forecaster_version
    FROM pred.forecasters
    WHERE forecaster_name = LOWER(sqlc.arg(forecaster_name)::TEXT)
        AND forecaster_version = LOWER(sqlc.arg(forecaster_version)::TEXT)
)
SELECT
    forecasts.forecast_uuid,
    forecasts.init_time_utc,
    forecasts.geometry_uuid,
    desired_forecaster.forecaster_name,
    desired_forecaster.forecaster_version,
    UUIDV7_EXTRACT_TIMESTAMP(forecasts.forecast_uuid) AS created_at_utc
FROM pred.forecasts AS forecasts
    INNER JOIN desired_forecaster USING (forecaster_id)
WHERE forecasts.geometry_uuid = $1
    AND forecasts.source_type_id = $2
    AND forecasts.init_time_utc BETWEEN
    sqlc.arg(start_timestamp)::TIMESTAMP
    AND sqlc.arg(end_timestamp)::TIMESTAMP;

-- name: ListPredictionsForForecast :many
/* ListPredictionsForForecast retrieves predicted generation values
 * for a given forecast as smallint percentages (sip) of capacity;
 * with 0 representing 0% and 30000 representing 100% of capacity.
 */
SELECT
    pg.horizon_mins,
    pg.p50_sip,
    pg.target_time_utc,
    pg.other_stats_fractions,
    pg.metadata,
    sv.capacity_watts
FROM pred.forecasts AS f
    INNER JOIN pred.predicted_generation_values AS pg USING (forecast_uuid)
    INNER JOIN loc.sources_mv AS sv USING (geometry_uuid, source_type_id)
WHERE f.forecast_uuid = $1
    AND sv.sys_period @> pg.target_time_utc;

-- name: ListPredictionsForLocation :many
/* ListPredictionsForLocation retrieves predicted generation values as a timeseries.
 * Multiple overlapping forecasts can make up the timeseries, so predictions with the same target time
 * are filtered by lowest allowable horizon (i.e. predicted closest to their target time).
 * Predicted values are smallint percentages (sip) of capcity;
 * with 0 representing 0% and 30000 representing 100% of capacity.
 */
WITH relevant_forecasts AS (
    /* First, filter the forecasts to return only those that have predictions within the target
     * period. */
    SELECT
        f.forecast_uuid,
        f.geometry_uuid,
        f.source_type_id,
        f.init_time_utc
    FROM pred.forecasts AS f
    WHERE f.geometry_uuid = $1
        AND f.source_type_id = $2
        AND f.forecaster_id = $3
        AND f.init_time_utc <= sqlc.arg(pivot_timestamp)::TIMESTAMP
        AND f.target_period && TSRANGE(
            sqlc.arg(start_timestamp_utc)::TIMESTAMP,
            sqlc.arg(end_timestamp_utc)::TIMESTAMP,
            '[]'
        )
),
ranked_predictions AS (
    /* Then, pull all the predicted values for said forecasts, and rank values with matching target
     * times according to their horizon. Only values with a horizon greater than or equal to the
     * input horizon are considered. */
    SELECT
        pg.horizon_mins,
        pg.p50_sip,
        pg.target_time_utc,
        pg.metadata,
        pg.other_stats_fractions,
        rf.init_time_utc,
        rf.forecast_uuid,
        rf.geometry_uuid,
        rf.source_type_id,
        ROW_NUMBER() OVER (
            PARTITION BY pg.target_time_utc
            ORDER BY pg.horizon_mins ASC
        ) AS rn
    FROM pred.predicted_generation_values AS pg
        INNER JOIN relevant_forecasts AS rf USING (forecast_uuid)
    WHERE
        pg.target_time_utc BETWEEN
        sqlc.arg(start_timestamp_utc)::TIMESTAMP
        AND sqlc.arg(end_timestamp_utc)::TIMESTAMP
        AND pg.horizon_mins >= sqlc.arg(horizon_mins)::INTEGER
)
SELECT
    /* For each target time, choose the value with the lowest allowable horizon. */
    rp.horizon_mins,
    rp.p50_sip,
    rp.target_time_utc,
    rp.init_time_utc,
    rp.metadata,
    rp.other_stats_fractions,
    sh.capacity_watts,
    UUIDV7_EXTRACT_TIMESTAMP(rp.forecast_uuid) AS created_at_utc
FROM ranked_predictions AS rp
    INNER JOIN loc.sources_mv AS sh USING (geometry_uuid, source_type_id)
WHERE rp.rn = 1
    AND sh.sys_period @> rp.target_time_utc
ORDER BY rp.target_time_utc ASC;

-- name: ListPredictionsAtTimeForLocations :many
/* ListPredictionsAtTimeForLocations retrieves predicted generation values as percentages
 * of capacity for a specific time and horizon.
 * This is useful for comparing predictions across multiple locations.
 * Predicted values are 16-bit integers, with 0 representing 0% and 30000 representing 100% of capacity.
 */
WITH relevant_forecasts AS (
    SELECT DISTINCT ON (f.geometry_uuid)
        f.forecast_uuid,
        f.geometry_uuid,
        f.source_type_id,
        f.init_time_utc
    FROM pred.forecasts AS f
    WHERE
        f.geometry_uuid = ANY(sqlc.arg(geometry_uuids)::UUID [])
        AND f.source_type_id = $1
        AND f.forecaster_id = $2
        AND f.target_period @> sqlc.arg(target_timestamp_utc)::TIMESTAMP
    ORDER BY f.geometry_uuid ASC, f.init_time_utc DESC
),
ranked_predictions AS (
    SELECT
        rf.forecast_uuid,
        rf.geometry_uuid,
        rf.source_type_id,
        pg.horizon_mins,
        pg.p50_sip,
        pg.target_time_utc,
        pg.metadata,
        pg.other_stats_fractions,
        ROW_NUMBER() OVER (
            PARTITION BY rf.geometry_uuid
            ORDER BY pg.horizon_mins ASC
        ) AS rn
    FROM relevant_forecasts AS rf
        INNER JOIN pred.predicted_generation_values AS pg USING (forecast_uuid)
    WHERE
        pg.target_time_utc = sqlc.arg(target_timestamp_utc)::TIMESTAMP
        AND pg.horizon_mins >= sqlc.arg(horizon_mins)::INTEGER
)
SELECT
    rp.forecast_uuid,
    rp.geometry_uuid,
    rp.source_type_id,
    rp.horizon_mins,
    rp.p50_sip,
    rp.target_time_utc,
    rp.metadata,
    rp.other_stats_fractions,
    sv.capacity_watts,
    sv.latitude,
    sv.longitude,
    sv.geometry_name
FROM ranked_predictions AS rp
    INNER JOIN loc.sources_mv AS sv USING (geometry_uuid, source_type_id)
WHERE rp.rn = 1
    AND sv.sys_period @> rp.target_time_utc;

-- name: GetWeekAverageDeltasForLocations :many
/* GetWeekAverageDeltasForLocations retrieves the average deltas between predicted and observed generation values
 * for a given source type, forecaster, and observer, across a week of forecasts made with the same init time.
 * The pivot timestamp is used to determine the week and init time of interest.
 * The results are grouped by location and horizon.
 */
WITH desired_init_times AS (
    SELECT (d.day::DATE + sqlc.arg(pivot_timestamp)::TIMESTAMP::TIME)::TIMESTAMP AS init_time_utc
    FROM
        GENERATE_SERIES(
            sqlc.arg(pivot_timestamp)::TIMESTAMP::DATE - INTERVAL '7 days',
            sqlc.arg(pivot_timestamp)::TIMESTAMP::DATE - INTERVAL '1 day',
            INTERVAL '1 day'
        ) AS d (day)
    ORDER BY d.day ASC
),
relevant_forecasts AS (
    SELECT
        f.forecast_uuid,
        f.init_time_utc,
        f.source_type_id,
        f.geometry_uuid,
        f.forecaster_id
    FROM pred.forecasts AS f
        INNER JOIN desired_init_times AS dit ON f.init_time_utc = dit.init_time_utc
    WHERE f.geometry_uuid = ANY(sqlc.arg(geometry_uuids)::UUID [])
        AND f.source_type_id = $1
        AND f.forecaster_id = $2
),
relevant_predicted_values AS (
    SELECT
        rf.geometry_uuid,
        rf.forecast_uuid,
        rf.source_type_id,
        pg.target_time_utc,
        pg.horizon_mins,
        pg.p50_sip
    FROM relevant_forecasts AS rf
        INNER JOIN pred.predicted_generation_values AS pg USING (forecast_uuid)
),
deltas AS (
    SELECT
        rv.geometry_uuid,
        rv.source_type_id,
        rv.forecast_uuid,
        rv.target_time_utc,
        rv.horizon_mins,
        rv.p50_sip - og.value_sip AS delta_sip
    FROM relevant_predicted_values AS rv
        LEFT OUTER JOIN obs.observed_generation_values AS og USING (geometry_uuid, source_type_id)
    WHERE
        og.observer_uuid = $3
        AND og.observation_timestamp_utc = rv.target_time_utc
)
SELECT
    d.geometry_uuid,
    d.horizon_mins,
    AVG(d.delta_sip) AS avg_delta_sip
FROM deltas AS d
GROUP BY d.geometry_uuid, d.horizon_mins
ORDER BY d.geometry_uuid, d.horizon_mins;

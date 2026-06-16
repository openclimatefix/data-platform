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
    forecast_uuid,
    geometry_uuid,
    source_type_id,
    forecaster_id,
    init_time_utc,
    value_resolution_mins,
    target_period,
    metadata,
    created_at_utc
) VALUES (
    UUIDV7($4::TIMESTAMP),
    $1,
    $2,
    $3,
    $4,
    $5,
    TSRANGE(
        $4::TIMESTAMP + MAKE_INTERVAL(mins => sqlc.arg(first_horizon_mins)::INTEGER),
        $4::TIMESTAMP + MAKE_INTERVAL(mins => sqlc.arg(last_horizon_mins)::INTEGER),
        '[]'
    ),
    CASE WHEN sqlc.arg(metadata)::JSONB = '{}'::JSONB THEN NULL ELSE sqlc.arg(metadata)::JSONB END,
    CASE
        WHEN sqlc.narg(created_at_utc)::TIMESTAMP IS NULL THEN CURRENT_TIMESTAMP ELSE
            sqlc.narg(created_at_utc)::TIMESTAMP
    END
) RETURNING
    forecast_uuid,
    init_time_utc,
    source_type_id,
    geometry_uuid,
    forecaster_id,
    target_period,
    metadata;

-- name: DeleteForecastByUUID :exec
DELETE FROM pred.forecasts
WHERE forecast_uuid = $1;

-- name: DeleteForecast :exec
WITH forecasts_to_delete AS (
    SELECT forecast_uuid FROM pred.forecasts AS f
    WHERE f.forecast_uuid >= UUIDV7_BOUNDARY(sqlc.arg(init_timestamp)::TIMESTAMP)
        AND f.forecast_uuid < UUIDV7_BOUNDARY(sqlc.arg(init_timestamp)::TIMESTAMP + INTERVAL '1 millisecond')
        AND f.geometry_uuid = $1
        AND f.source_type_id = $2
        AND f.forecaster_id = $3
)
DELETE FROM pred.forecasts
WHERE forecast_uuid IN (SELECT forecast_uuid FROM forecasts_to_delete);

-- name: CreatePredictedValues :copyfrom
/* CreatePredictedValues inserts predicted generation values using
 * postgres COPY protocol, making it the fastest way to perform large inserts of predictions.
 * Input p-values are expected as smallint percentages (sip) of capacity,
 * with 0 representing 0% and 30000 representing 100% of capacity.
 */
INSERT INTO pred.predicted_generation_values (
    horizon_mins, p50_sip, p10_sip, p90_sip, forecast_uuid
) VALUES (
    $1, $2, $3, $4, $5
);

-- name: ListPredictionsForForecasts :many
/* ListPredictionsForForecasts retrieves all predicted generation values for a given location,
 * source type, and dynamic list of forecasters within a time window.
 * Note that this does not return ordered results for speed. Ordering is up to the client.
 */
WITH requested_forecasters AS (
    SELECT
        UNNEST(sqlc.arg(forecaster_names)::TEXT []) AS fname,
        UNNEST(sqlc.arg(forecaster_versions)::TEXT []) AS fversion
),
matched_forecasters AS (
    SELECT
        f.forecaster_id,
        f.forecaster_name,
        f.forecaster_version
    FROM pred.forecasters AS f
        INNER JOIN requested_forecasters AS rf
        ON f.forecaster_name = LOWER(rf.fname)
            AND f.forecaster_version = LOWER(rf.fversion)
)
SELECT
    mf.forecaster_name,
    mf.forecaster_version,
    f.created_at_utc,
    pg.horizon_mins,
    pg.p10_sip,
    pg.p50_sip,
    pg.p90_sip,
    sv.capacity_watts,
    f.metadata,
    UUIDV7_EXTRACT_TIMESTAMP(f.forecast_uuid)::TIMESTAMP AS init_time_utc,
    (
        UUIDV7_EXTRACT_TIMESTAMP(pg.forecast_uuid)::TIMESTAMP + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER)
    )::TIMESTAMP AS target_time_utc
FROM pred.forecasts AS f
    INNER JOIN matched_forecasters AS mf USING (forecaster_id)
    INNER JOIN pred.predicted_generation_values AS pg
    ON f.forecast_uuid = pg.forecast_uuid
        AND pg.forecast_uuid >= UUIDV7_BOUNDARY(sqlc.arg(start_timestamp)::TIMESTAMP)
        AND pg.forecast_uuid < UUIDV7_BOUNDARY(sqlc.arg(end_timestamp)::TIMESTAMP + INTERVAL '1 millisecond')
    LEFT OUTER JOIN LATERAL (
        SELECT capacity_watts
        FROM loc.sources_mv AS s
        WHERE s.geometry_uuid = f.geometry_uuid
            AND s.source_type_id = f.source_type_id
            AND s.sys_period
            @> (UUIDV7_EXTRACT_TIMESTAMP(pg.forecast_uuid)::TIMESTAMP + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER))
        LIMIT 1
    ) AS sv ON TRUE
WHERE f.geometry_uuid = sqlc.arg(geometry_uuid)::UUID
    AND f.source_type_id = sqlc.arg(source_type_id)::SMALLINT
    AND f.forecast_uuid >= UUIDV7_BOUNDARY(sqlc.arg(start_timestamp)::TIMESTAMP)
    AND f.forecast_uuid < UUIDV7_BOUNDARY(sqlc.arg(end_timestamp)::TIMESTAMP + INTERVAL '1 millisecond');

-- name: GetLatestForecastsAtHorizonSincePivot :many
/* GetLatestForecastAtHorizonSincePivot retrieves the latest forecasts for a given location
 * and source type made by each individual forecaster name.
 * Only forecasts that are older than the pivot time minus the specified horizon are considered.
 *
 * The LATERAL cross join defers querying the forecasts table until each forecaster id is known.
 */
SELECT DISTINCT ON (fr.forecaster_name)
    f.forecast_uuid,
    f.init_time_utc,
    f.created_at_utc,
    f.source_type_id,
    f.geometry_uuid,
    f.metadata,
    fr.forecaster_id,
    fr.forecaster_name,
    fr.forecaster_version
FROM pred.forecasters AS fr
    CROSS JOIN LATERAL (
        SELECT
            forecast_uuid,
            created_at_utc,
            source_type_id,
            geometry_uuid,
            metadata,
            UUIDV7_EXTRACT_TIMESTAMP(forecast_uuid) AS init_time_utc
        FROM pred.forecasts
        WHERE geometry_uuid = $1
            AND source_type_id = $2
            AND forecaster_id = fr.forecaster_id
            AND forecast_uuid < UUIDV7_BOUNDARY(
                sqlc.arg(pivot_timestamp)::TIMESTAMP - MAKE_INTERVAL(
                    mins => sqlc.arg(horizon_mins)::INTEGER
                ) + INTERVAL '1 millisecond'
            )
            AND created_at_utc <= COALESCE(sqlc.narg(pivot_timestamp)::TIMESTAMP, CURRENT_TIMESTAMP)
        ORDER BY forecast_uuid DESC
        LIMIT 1
    ) AS f
ORDER BY fr.forecaster_name ASC, f.init_time_utc DESC;

-- name: ListPredictionsForLocation :many
/* ListPredictionsForLocation retrieves predicted generation values as a timeseries.
 * Multiple overlapping forecasts can make up the timeseries, so predictions with the same target time
 * are filtered by lowest allowable horizon (i.e. predicted closest to their target time).
 * Predicted values are smallint percentages (sip) of capcity;
 * with 0 representing 0% and 30000 representing 100% of capacity.
 *
 * Note that the 3 day intervals are due to our forecasts only going out to 2 days.
 * If we increase that horizon, these will need to be increased.
 */
WITH allowed_forecasts_overlapping_window AS (
    SELECT
        f.forecast_uuid,
        f.geometry_uuid,
        f.source_type_id,
        f.created_at_utc,
        f.metadata,
        UUIDV7_EXTRACT_TIMESTAMP(f.forecast_uuid)::TIMESTAMP AS init_time_utc
    FROM pred.forecasts AS f
    WHERE f.geometry_uuid = $1
        AND f.source_type_id = $2
        AND f.forecaster_id = $3
        AND f.forecast_uuid >= UUIDV7_BOUNDARY(
            sqlc.arg(start_timestamp_utc)::TIMESTAMP - INTERVAL '3 days'
        )
        AND f.forecast_uuid < UUIDV7_BOUNDARY(
            sqlc.arg(end_timestamp_utc)::TIMESTAMP
            - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::INTEGER)
            + INTERVAL '1 millisecond'
        )
        AND f.created_at_utc <= COALESCE(sqlc.narg(pivot_timestamp)::TIMESTAMP, CURRENT_TIMESTAMP)
        AND f.target_period && TSRANGE(
            sqlc.arg(start_timestamp_utc)::TIMESTAMP,
            sqlc.arg(end_timestamp_utc)::TIMESTAMP,
            '[]'
        )
),
winning_predictions AS (
    SELECT DISTINCT ON (
        UUIDV7_EXTRACT_TIMESTAMP(pg.forecast_uuid)::TIMESTAMP + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER)
    )
        fow.forecast_uuid,
        fow.init_time_utc,
        fow.created_at_utc,
        fow.geometry_uuid,
        fow.source_type_id,
        pg.horizon_mins,
        pg.p10_sip,
        pg.p50_sip,
        pg.p90_sip,
        fow.metadata,
        (
            UUIDV7_EXTRACT_TIMESTAMP(pg.forecast_uuid)::TIMESTAMP + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER)
        )::TIMESTAMP AS target_time_utc
    FROM allowed_forecasts_overlapping_window AS fow
        INNER JOIN pred.predicted_generation_values AS pg USING (forecast_uuid)
    WHERE (
        UUIDV7_EXTRACT_TIMESTAMP(pg.forecast_uuid)::TIMESTAMP + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER)
    ) BETWEEN sqlc.arg(start_timestamp_utc)::TIMESTAMP AND sqlc.arg(end_timestamp_utc)::TIMESTAMP
    AND pg.horizon_mins >= sqlc.arg(horizon_mins)::INTEGER
    -- Sorting by decreasing init time ensures the DISTINCT captures the lowest allowed horizon
    ORDER BY
        (UUIDV7_EXTRACT_TIMESTAMP(pg.forecast_uuid)::TIMESTAMP + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER)) ASC,
        fow.init_time_utc DESC
)
SELECT
    wp.horizon_mins,
    wp.p10_sip,
    wp.p50_sip,
    wp.p90_sip,
    wp.target_time_utc,
    wp.metadata,
    wp.init_time_utc,
    wp.created_at_utc,
    sv.capacity_watts,
    sv.latitude,
    sv.longitude,
    sv.geometry_name
FROM winning_predictions AS wp
    INNER JOIN loc.sources_mv AS sv USING (geometry_uuid, source_type_id)
WHERE sv.sys_period @> wp.target_time_utc
ORDER BY wp.target_time_utc ASC;

-- name: ListPredictionsAtTimeForLocations :many
/* ListPredictionsAtTimeForLocations retrieves predicted generation values as percentages
 * of capacity for a specific time and horizon.
 * This is useful for comparing predictions across multiple locations.
 * Predicted values are 16-bit integers, with 0 representing 0% and 30000 representing 100% of capacity.
 *
 * Note that the 3 day intervals are due to our forecasts only going out to 2 days.
 * If we increase that horizon, these will need to be increased.
 */
-- name: ListPredictionsAtTimeForLocations :many
WITH target_locations AS (
    SELECT UNNEST(sqlc.arg(geometry_uuids)::UUID []) AS geometry_uuid
),
latest_allowed_forecast_per_location AS (
    SELECT
        lf.forecast_uuid,
        tl.geometry_uuid::UUID AS geometry_uuid, -- again, SQLC complains without this
        lf.source_type_id,
        lf.created_at_utc,
        lf.metadata,
        UUIDV7_EXTRACT_TIMESTAMP(lf.forecast_uuid)::TIMESTAMP AS init_time_utc
    FROM target_locations AS tl
        CROSS JOIN LATERAL (
            SELECT
                f.forecast_uuid,
                f.source_type_id,
                f.created_at_utc,
                f.metadata
            FROM pred.forecasts AS f
            WHERE f.geometry_uuid = tl.geometry_uuid
                AND f.source_type_id = $1
                AND f.forecaster_id = $2
                AND f.target_period @> sqlc.arg(target_timestamp_utc)::TIMESTAMP
                AND f.forecast_uuid >= UUIDV7_BOUNDARY(
                    sqlc.arg(target_timestamp_utc)::TIMESTAMP - INTERVAL '3 days'
                )
                AND f.forecast_uuid < UUIDV7_BOUNDARY(
                    sqlc.arg(target_timestamp_utc)::TIMESTAMP
                    - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::INTEGER)
                    + INTERVAL '1 millisecond'
                )
                AND f.created_at_utc
                <= COALESCE(sqlc.narg(pivot_timestamp)::TIMESTAMP, CURRENT_TIMESTAMP)
            ORDER BY f.forecast_uuid DESC
            LIMIT 1
        ) AS lf
)
SELECT
    laf.forecast_uuid,
    laf.geometry_uuid,
    laf.source_type_id,
    pg.horizon_mins,
    pg.p10_sip,
    pg.p50_sip,
    pg.p90_sip,
    laf.created_at_utc,
    laf.init_time_utc,
    sv.capacity_watts,
    sv.latitude,
    sv.longitude,
    sv.geometry_name,
    laf.metadata,
    sqlc.arg(target_timestamp_utc)::TIMESTAMP AS target_time_utc
FROM latest_allowed_forecast_per_location AS laf
    INNER JOIN pred.predicted_generation_values AS pg USING (forecast_uuid)
    INNER JOIN loc.sources_mv AS sv USING (geometry_uuid, source_type_id)
WHERE
    (UUIDV7_EXTRACT_TIMESTAMP(pg.forecast_uuid)::TIMESTAMP + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER))
    = sqlc.arg(target_timestamp_utc)::TIMESTAMP
    AND sv.sys_period @> sqlc.arg(target_timestamp_utc)::TIMESTAMP;

-- name: GetWeekAverageDeltasForLocations :many
/* GetWeekAverageDeltasForLocations retrieves the average deltas between predicted and observed generation values
 * for a given source type, forecaster, and observer, across a week of forecasts made with the same init time.
 * The pivot timestamp is used to determine the week and init time of interest. The results are
 * grouped by location and horizon. MATERIALIZED is used because the count assumptions made by postgres on the
 * CTEs are unreliable thanks to the UUIDV7_EXTRACT_TIMESTAMP function call.
 */
WITH relevant_forecasts AS (
    SELECT
        f.forecast_uuid,
        f.source_type_id,
        f.geometry_uuid,
        f.forecaster_id
    FROM pred.forecasts AS f
    WHERE f.geometry_uuid = $4
        AND f.source_type_id = $1
        AND f.forecaster_id = $2
        AND f.forecast_uuid >= UUIDV7_BOUNDARY(sqlc.arg(pivot_timestamp)::TIMESTAMP - INTERVAL '8 days')
        AND f.forecast_uuid < UUIDV7_BOUNDARY(sqlc.arg(pivot_timestamp)::TIMESTAMP + INTERVAL '1 millisecond')
        AND UUIDV7_EXTRACT_TIMESTAMP(f.forecast_uuid)::TIME = sqlc.arg(pivot_timestamp)::TIMESTAMP::TIME
),
relevant_predicted_values AS MATERIALIZED (
    SELECT
        rf.geometry_uuid,
        rf.source_type_id,
        pg.horizon_mins,
        pg.p50_sip,
        (
            UUIDV7_EXTRACT_TIMESTAMP(pg.forecast_uuid)::TIMESTAMP + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER)
        )::TIMESTAMP AS target_time_utc
    FROM relevant_forecasts AS rf
        INNER JOIN pred.predicted_generation_values AS pg USING (forecast_uuid)
    WHERE pg.forecast_uuid >= UUIDV7_BOUNDARY(sqlc.arg(pivot_timestamp)::TIMESTAMP - INTERVAL '8 days')
        AND pg.forecast_uuid < UUIDV7_BOUNDARY(sqlc.arg(pivot_timestamp)::TIMESTAMP + INTERVAL '1 millisecond')
),
relevant_observations AS MATERIALIZED (
    SELECT
        geometry_uuid,
        source_type_id,
        observation_timestamp_utc,
        value_sip
    FROM obs.observed_generation_values
    WHERE observer_uuid = $3
        AND geometry_uuid = $4
        AND observation_timestamp_utc >= sqlc.arg(pivot_timestamp)::TIMESTAMP - INTERVAL '8 days'
        AND observation_timestamp_utc < sqlc.arg(pivot_timestamp)::TIMESTAMP + INTERVAL '1 millisecond'
)
SELECT
    rv.geometry_uuid,
    rv.horizon_mins,
    AVG(rv.p50_sip - og.value_sip) AS avg_delta_sip
FROM relevant_predicted_values AS rv
    INNER JOIN relevant_observations AS og
    ON rv.geometry_uuid = og.geometry_uuid
        AND rv.source_type_id = og.source_type_id
        AND rv.target_time_utc = og.observation_timestamp_utc
GROUP BY rv.geometry_uuid, rv.horizon_mins
ORDER BY rv.geometry_uuid, rv.horizon_mins;

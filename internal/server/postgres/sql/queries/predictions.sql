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

-- name: CreateForecasts :copyfrom
INSERT INTO pred.forecasts (
    forecast_uuid,
    geometry_uuid,
    source_type_id,
    forecaster_id,
    init_time_utc,
    value_resolution_mins,
    target_period,
    metadata,
    created_at_utc,
    p02_sips,
    p10_sips,
    p25_sips,
    p50_sips,
    p75_sips,
    p90_sips,
    p98_sips
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
);

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
    horizon_mins, p50_sip, p10_sip, p90_sip, forecast_uuid, p02_sip, p98_sip, p25_sip, p75_sip
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9
);

-- name: ListPredictionsForForecasts :many
/* ListPredictionsForForecasts retrieves all predicted generation values for a given location,
 * source type, and dynamic list of forecasters within a time window.
 * Note that this does not return ordered results for speed. Ordering is up to the client.
 *
 * Currently this is two queries in one, as the application in this version's state can have
 * values stored either in arrays or in the legacy predicted_generation_values table.
 * When everything is migrated to arrays, the second branch can be removed.
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
),
matched_forecasts AS (
    SELECT
        f.forecast_uuid, f.geometry_uuid, f.source_type_id, f.created_at_utc, f.metadata,
        f.init_time_utc, f.value_resolution_mins,
        LOWER(f.target_period) AS first_target_utc,
        f.p02_sips, f.p10_sips, f.p25_sips, f.p50_sips, f.p75_sips, f.p90_sips, f.p98_sips,
        mf.forecaster_name, mf.forecaster_version
    FROM pred.forecasts AS f
        INNER JOIN matched_forecasters AS mf USING (forecaster_id)
    WHERE f.geometry_uuid = sqlc.arg(geometry_uuid)::UUID
        AND f.source_type_id = sqlc.arg(source_type_id)::SMALLINT
        AND f.forecast_uuid >= UUIDV7_BOUNDARY(sqlc.arg(start_timestamp)::TIMESTAMP)
        AND f.forecast_uuid < UUIDV7_BOUNDARY(sqlc.arg(end_timestamp)::TIMESTAMP + INTERVAL '1 millisecond')
),
expanded_array AS (
    SELECT
        mfc.forecaster_name, mfc.forecaster_version, mfc.created_at_utc, mfc.metadata,
        mfc.geometry_uuid, mfc.source_type_id, mfc.init_time_utc,
        (EXTRACT(EPOCH FROM (mfc.first_target_utc - mfc.init_time_utc)) / 60
            + (o.ord - 1) * mfc.value_resolution_mins)::SMALLINT AS horizon_mins,
        (mfc.first_target_utc + MAKE_INTERVAL(mins =>
            ((o.ord - 1) * mfc.value_resolution_mins)::INTEGER))::TIMESTAMP AS target_time_utc,
        o.p50_sip,
        mfc.p02_sips[o.ord] AS p02_sip,
        mfc.p10_sips[o.ord] AS p10_sip,
        mfc.p25_sips[o.ord] AS p25_sip,
        mfc.p75_sips[o.ord] AS p75_sip,
        mfc.p90_sips[o.ord] AS p90_sip,
        mfc.p98_sips[o.ord] AS p98_sip
    FROM matched_forecasts AS mfc
    CROSS JOIN LATERAL unnest(mfc.p50_sips) WITH ORDINALITY AS o(p50_sip, ord)
    WHERE mfc.p50_sips IS NOT NULL
),
expanded_legacy AS (
    SELECT
        mfc.forecaster_name, mfc.forecaster_version, mfc.created_at_utc, mfc.metadata,
        mfc.geometry_uuid, mfc.source_type_id, mfc.init_time_utc,
        pg.horizon_mins,
        (mfc.init_time_utc + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER))::TIMESTAMP
            AS target_time_utc,
        pg.p50_sip, pg.p02_sip, pg.p10_sip, pg.p25_sip, pg.p75_sip, pg.p90_sip, pg.p98_sip
    FROM matched_forecasts AS mfc
        INNER JOIN pred.predicted_generation_values AS pg
        ON mfc.forecast_uuid = pg.forecast_uuid
            AND pg.forecast_uuid >= UUIDV7_BOUNDARY(sqlc.arg(start_timestamp)::TIMESTAMP)
            AND pg.forecast_uuid < UUIDV7_BOUNDARY(sqlc.arg(end_timestamp)::TIMESTAMP + INTERVAL '1 millisecond')
    WHERE mfc.p50_sips IS NULL
),
expanded AS (
    SELECT * FROM expanded_array
    UNION ALL
    SELECT * FROM expanded_legacy
)
/* Column order here is load-bearing: StreamForecastData scans these positionally. */
SELECT
    e.forecaster_name,
    e.forecaster_version,
    e.created_at_utc,
    e.horizon_mins,
    e.p02_sip,
    e.p10_sip,
    e.p25_sip,
    e.p50_sip,
    e.p75_sip,
    e.p90_sip,
    e.p98_sip,
    sv.capacity_watts,
    e.metadata,
    e.init_time_utc,
    e.target_time_utc
FROM expanded AS e
    LEFT OUTER JOIN LATERAL (
        SELECT capacity_watts
        FROM loc.sources_mv AS s
        WHERE s.geometry_uuid = e.geometry_uuid
            AND s.source_type_id = e.source_type_id
            AND s.sys_period @> e.target_time_utc
        LIMIT 1
    ) AS sv ON TRUE;

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
            -- Without a lower bound the range is (-infinity, pivot), so MergeAppend has to open
            -- every partition including the historical default one.
            AND forecast_uuid >= UUIDV7_BOUNDARY(
                sqlc.arg(pivot_timestamp)::TIMESTAMP - INTERVAL '7 days'
            )
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
/* ListPredictionsForLocation retrieves all predicted generation values for a given location,
 * source type, and forecaster within a time window.
 *
 * Currently this is two queries in one, as the application in this version's state can have
 * values stored either in arrays or in the legacy predicted_generation_values table.
 * When everything is migrated to arrays, the second branch can be removed.
 */
WITH allowed_forecasts AS (
    SELECT
        f.forecast_uuid, f.geometry_uuid, f.source_type_id, f.created_at_utc, f.metadata,
        f.value_resolution_mins, f.init_time_utc,
        LOWER(f.target_period) AS first_target_utc,
        f.p02_sips, f.p10_sips, f.p25_sips, f.p50_sips, f.p75_sips, f.p90_sips, f.p98_sips
    FROM pred.forecasts AS f
    WHERE f.geometry_uuid = $1
        AND f.source_type_id = $2
        AND f.forecaster_id = $3
        AND f.forecast_uuid >= UUIDV7_BOUNDARY(
            sqlc.arg(start_timestamp_utc)::TIMESTAMP - INTERVAL '3 days')
        AND f.forecast_uuid < UUIDV7_BOUNDARY(
            sqlc.arg(end_timestamp_utc)::TIMESTAMP
            - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::INTEGER)
            + INTERVAL '1 millisecond')
        AND f.created_at_utc <= COALESCE(sqlc.narg(pivot_timestamp)::TIMESTAMP, CURRENT_TIMESTAMP)
        AND f.target_period && TSRANGE(
            sqlc.arg(start_timestamp_utc)::TIMESTAMP,
            sqlc.arg(end_timestamp_utc)::TIMESTAMP, '[]')
),
sliced AS (
    /* Convert the target-time window and minimum horizon into an array index range.
     * This means that a forecast with a window that only partially overlaps the requested
     * window will only be partially expanded. */
    SELECT af.*,
        GREATEST(1, CEIL(EXTRACT(EPOCH FROM (GREATEST(
            sqlc.arg(start_timestamp_utc)::TIMESTAMP,
            af.init_time_utc + MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::INTEGER)
        ) - af.first_target_utc)) / 60.0 / af.value_resolution_mins)::INTEGER + 1) AS lo,
        LEAST(ARRAY_LENGTH(af.p50_sips, 1), FLOOR(EXTRACT(EPOCH FROM (
            sqlc.arg(end_timestamp_utc)::TIMESTAMP - af.first_target_utc
        )) / 60.0 / af.value_resolution_mins)::INTEGER + 1) AS hi
    FROM allowed_forecasts AS af
    WHERE af.p50_sips IS NOT NULL
),
expanded_array AS (
    /* Expand the sliced arrays into rows, with each row representing a single
     * target time and its associated predicted values. */
    SELECT
        s.forecast_uuid, s.init_time_utc, s.created_at_utc, s.metadata,
        s.geometry_uuid, s.source_type_id,
        (s.first_target_utc + MAKE_INTERVAL(mins =>
            ((s.lo + o.ord - 2) * s.value_resolution_mins)::INTEGER))::TIMESTAMP
            AS target_time_utc,
        (EXTRACT(EPOCH FROM (
            s.first_target_utc
            + MAKE_INTERVAL(mins => ((s.lo + o.ord - 2) * s.value_resolution_mins)::INTEGER)
            - s.init_time_utc
        )) / 60)::SMALLINT AS horizon_mins,
        o.p50_sip::SMALLINT AS p50_sip,
        s.p02_sips[s.lo + o.ord - 1]::SMALLINT AS p02_sip,
        s.p10_sips[s.lo + o.ord - 1]::SMALLINT AS p10_sip,
        s.p25_sips[s.lo + o.ord - 1]::SMALLINT AS p25_sip,
        s.p75_sips[s.lo + o.ord - 1]::SMALLINT AS p75_sip,
        s.p90_sips[s.lo + o.ord - 1]::SMALLINT AS p90_sip,
        s.p98_sips[s.lo + o.ord - 1]::SMALLINT AS p98_sip
    FROM sliced AS s
    CROSS JOIN LATERAL unnest(s.p50_sips[s.lo:s.hi])
        WITH ORDINALITY AS o(p50_sip, ord)
    WHERE s.hi >= s.lo
),
expanded_legacy AS (
    /* Forecasts whose partition has not yet been rebuilt into arrays. Column order must match
     * expanded_array exactly - UNION ALL matches by position, not by name. */
    SELECT
        af.forecast_uuid, af.init_time_utc, af.created_at_utc, af.metadata,
        af.geometry_uuid, af.source_type_id,
        (af.init_time_utc + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER))::TIMESTAMP
            AS target_time_utc,
        pg.horizon_mins,
        pg.p50_sip, pg.p02_sip, pg.p10_sip, pg.p25_sip, pg.p75_sip, pg.p90_sip, pg.p98_sip
    FROM allowed_forecasts AS af
        INNER JOIN pred.predicted_generation_values AS pg
        ON af.forecast_uuid = pg.forecast_uuid
            /* Repeating the bounds from allowed_forecasts lets the planner prune partitions of
             * predicted_generation_values statically. Without them the equijoin alone only prunes
             * at runtime, and only if a nested loop is chosen over a hash join. */
            AND pg.forecast_uuid >= UUIDV7_BOUNDARY(
                sqlc.arg(start_timestamp_utc)::TIMESTAMP - INTERVAL '3 days')
            AND pg.forecast_uuid < UUIDV7_BOUNDARY(
                sqlc.arg(end_timestamp_utc)::TIMESTAMP
                - MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::INTEGER)
                + INTERVAL '1 millisecond')
    WHERE af.p50_sips IS NULL
        AND (af.init_time_utc + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER))
            BETWEEN sqlc.arg(start_timestamp_utc)::TIMESTAMP
            AND sqlc.arg(end_timestamp_utc)::TIMESTAMP
        AND pg.horizon_mins >= sqlc.arg(horizon_mins)::INTEGER
),
expanded AS (
    SELECT * FROM expanded_array
    UNION ALL
    SELECT * FROM expanded_legacy
),
winning_predictions AS (
    /* ordering by descending forecast_uuid means the lowest horizons are selected first,
     * since init times are encoded within it. */
    SELECT DISTINCT ON (target_time_utc) *
    FROM expanded
    ORDER BY target_time_utc ASC, forecast_uuid DESC
)
SELECT
    wp.horizon_mins, wp.p02_sip, wp.p25_sip, wp.p10_sip, wp.p50_sip,
    wp.p75_sip, wp.p90_sip, wp.p98_sip,
    wp.target_time_utc, wp.metadata, wp.init_time_utc, wp.created_at_utc,
    sv.capacity_watts, sv.latitude, sv.longitude, sv.geometry_name
FROM winning_predictions AS wp
    INNER JOIN loc.sources_mv AS sv USING (geometry_uuid, source_type_id)
WHERE sv.sys_period @> wp.target_time_utc
ORDER BY wp.target_time_utc ASC;

-- name: ListPredictionsAtTimeForLocations :many
/* PostgreSQL returns NULL on an out of bounds array index. As such, ARRAY_LENGTH is used
 * to guard against this.
 */
WITH target_locations AS (
    SELECT UNNEST(sqlc.arg(geometry_uuids)::UUID []) AS geometry_uuid
),
latest_allowed_forecast_per_location AS (
    SELECT
        lf.forecast_uuid,
        tl.geometry_uuid::UUID AS geometry_uuid,
        lf.source_type_id,
        lf.created_at_utc,
        lf.metadata,
        lf.init_time_utc,
        lf.value_resolution_mins,
        LOWER(lf.target_period) AS first_target_utc,
        lf.p02_sips, lf.p10_sips, lf.p25_sips, lf.p50_sips,
        lf.p75_sips, lf.p90_sips, lf.p98_sips
    FROM target_locations AS tl
        CROSS JOIN LATERAL (
            SELECT
                f.forecast_uuid, f.source_type_id, f.created_at_utc, f.metadata,
                f.init_time_utc, f.value_resolution_mins, f.target_period,
                f.p02_sips, f.p10_sips, f.p25_sips, f.p50_sips,
                f.p75_sips, f.p90_sips, f.p98_sips
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
),
indexed AS (
    /* target_time = first_target_utc + (i - 1) * value_resolution_mins,
     * so i = (target - first_target) / resolution + 1. Only meaningful when p50_sips is
     * populated; the legacy branch below keys on horizon_mins instead. */
    SELECT laf.*,
        (EXTRACT(EPOCH FROM (
            sqlc.arg(target_timestamp_utc)::TIMESTAMP - laf.first_target_utc
        )) / 60 / laf.value_resolution_mins)::INTEGER + 1 AS idx,
        (EXTRACT(EPOCH FROM (
            sqlc.arg(target_timestamp_utc)::TIMESTAMP - laf.init_time_utc
        )) / 60)::SMALLINT AS horizon_mins
    FROM latest_allowed_forecast_per_location AS laf
)
SELECT
    i.forecast_uuid,
    i.geometry_uuid,
    i.source_type_id,
    i.horizon_mins,
    COALESCE(i.p02_sips[i.idx], legacy.p02_sip) AS p02_sip,
    COALESCE(i.p10_sips[i.idx], legacy.p10_sip) AS p10_sip,
    COALESCE(i.p25_sips[i.idx], legacy.p25_sip) AS p25_sip,
    COALESCE(i.p50_sips[i.idx], legacy.p50_sip) AS p50_sip,
    COALESCE(i.p75_sips[i.idx], legacy.p75_sip) AS p75_sip,
    COALESCE(i.p90_sips[i.idx], legacy.p90_sip) AS p90_sip,
    COALESCE(i.p98_sips[i.idx], legacy.p98_sip) AS p98_sip,
    i.created_at_utc,
    i.init_time_utc,
    sv.capacity_watts,
    sv.latitude,
    sv.longitude,
    sv.geometry_name,
    i.metadata,
    sqlc.arg(target_timestamp_utc)::TIMESTAMP AS target_time_utc
FROM indexed AS i
    INNER JOIN loc.sources_mv AS sv USING (geometry_uuid, source_type_id)
    /* The p50_sips IS NULL test sits inside the subquery, not in an ON clause: a LEFT JOIN's ON
     * condition filters the result but does not stop the subquery being evaluated, so putting it
     * there would probe predicted_generation_values for migrated forecasts too. */
    LEFT JOIN LATERAL (
        SELECT pg.p02_sip, pg.p10_sip, pg.p25_sip, pg.p50_sip, pg.p75_sip, pg.p90_sip, pg.p98_sip
        FROM pred.predicted_generation_values AS pg
        WHERE i.p50_sips IS NULL
            AND pg.forecast_uuid = i.forecast_uuid
            AND pg.horizon_mins = i.horizon_mins
    ) AS legacy ON TRUE
WHERE (
    (i.p50_sips IS NOT NULL AND i.idx BETWEEN 1 AND ARRAY_LENGTH(i.p50_sips, 1)
     AND MOD((EXTRACT(EPOCH FROM (sqlc.arg(target_timestamp_utc)::TIMESTAMP - i.first_target_utc)) / 60)::NUMERIC, i.value_resolution_mins::NUMERIC) = 0)
    OR (i.p50_sips IS NULL AND legacy.p50_sip IS NOT NULL)
)
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
        f.forecaster_id,
        f.init_time_utc,
        f.value_resolution_mins,
        LOWER(f.target_period) AS first_target_utc,
        f.p50_sips
    FROM pred.forecasts AS f
    WHERE f.geometry_uuid = $4
        AND f.source_type_id = $1
        AND f.forecaster_id = $2
        AND f.forecast_uuid >= UUIDV7_BOUNDARY(sqlc.arg(pivot_timestamp)::TIMESTAMP - INTERVAL '8 days')
        AND f.forecast_uuid < UUIDV7_BOUNDARY(sqlc.arg(pivot_timestamp)::TIMESTAMP + INTERVAL '1 millisecond')
        AND f.init_time_utc::TIME = sqlc.arg(pivot_timestamp)::TIMESTAMP::TIME
),
expanded_array AS (
    SELECT
        rf.geometry_uuid,
        rf.source_type_id,
        (EXTRACT(EPOCH FROM (rf.first_target_utc - rf.init_time_utc)) / 60
            + (o.ord - 1) * rf.value_resolution_mins)::SMALLINT AS horizon_mins,
        o.p50_sip,
        (rf.first_target_utc + MAKE_INTERVAL(mins =>
            ((o.ord - 1) * rf.value_resolution_mins)::INTEGER))::TIMESTAMP AS target_time_utc
    FROM relevant_forecasts AS rf
    CROSS JOIN LATERAL unnest(rf.p50_sips) WITH ORDINALITY AS o(p50_sip, ord)
    WHERE rf.p50_sips IS NOT NULL
),
expanded_legacy AS (
    SELECT
        rf.geometry_uuid,
        rf.source_type_id,
        pg.horizon_mins,
        pg.p50_sip,
        (rf.init_time_utc + MAKE_INTERVAL(mins => pg.horizon_mins::INTEGER))::TIMESTAMP
            AS target_time_utc
    FROM relevant_forecasts AS rf
        INNER JOIN pred.predicted_generation_values AS pg USING (forecast_uuid)
    WHERE rf.p50_sips IS NULL
        AND pg.forecast_uuid >= UUIDV7_BOUNDARY(sqlc.arg(pivot_timestamp)::TIMESTAMP - INTERVAL '8 days')
        AND pg.forecast_uuid < UUIDV7_BOUNDARY(sqlc.arg(pivot_timestamp)::TIMESTAMP + INTERVAL '1 millisecond')
),
relevant_predicted_values AS MATERIALIZED (
    SELECT * FROM expanded_array
    UNION ALL
    SELECT * FROM expanded_legacy
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

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
    ARRAY_LENGTH(sqlc.arg(forecaster_names)::TEXT[], 1) IS NULL
    OR forecaster_name = ANY(sqlc.arg(forecaster_names)::TEXT[])
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

-- name: ListPredictionsForForecasts :many
/* ListPredictionsForForecasts retrieves all predicted generation values for a given location,
 * source type, and dynamic list of forecasters within a time window.
 * Note that this does not return ordered results for speed. Ordering is up to the client.
 */
WITH requested_forecasters AS (
    SELECT
        UNNEST(sqlc.arg(forecaster_names)::TEXT[]) AS fname,
        UNNEST(sqlc.arg(forecaster_versions)::TEXT[]) AS fversion
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
        f.forecast_uuid,
        f.geometry_uuid,
        f.source_type_id,
        f.created_at_utc,
        f.metadata,
        f.init_time_utc,
        f.value_resolution_mins,
        f.p02_sips,
        f.p10_sips,
        f.p25_sips,
        f.p50_sips,
        f.p75_sips,
        f.p90_sips,
        f.p98_sips,
        mf.forecaster_name,
        mf.forecaster_version,
        LOWER(f.target_period) AS first_target_utc
    FROM pred.forecasts AS f
        INNER JOIN matched_forecasters AS mf USING (forecaster_id)
    WHERE f.geometry_uuid = sqlc.arg(geometry_uuid)::UUID
        AND f.source_type_id = sqlc.arg(source_type_id)::SMALLINT
        AND f.forecast_uuid >= UUIDV7_BOUNDARY(sqlc.arg(start_timestamp)::TIMESTAMP)
        AND f.forecast_uuid < UUIDV7_BOUNDARY(sqlc.arg(end_timestamp)::TIMESTAMP + INTERVAL '1 millisecond')
),
expanded AS (
    SELECT
        mfc.forecaster_name,
        mfc.forecaster_version,
        mfc.created_at_utc,
        mfc.metadata,
        mfc.geometry_uuid,
        mfc.source_type_id,
        mfc.init_time_utc,
        o.p50_sip,
        (
            EXTRACT(EPOCH FROM (mfc.first_target_utc - mfc.init_time_utc)) / 60
            + (o.ord - 1) * mfc.value_resolution_mins
        )::SMALLINT AS horizon_mins,
        (mfc.first_target_utc + MAKE_INTERVAL(
            mins =>
            ((o.ord - 1) * mfc.value_resolution_mins)::INTEGER
        ))::TIMESTAMP AS target_time_utc,
        mfc.p02_sips[o.ord] AS p02_sip,
        mfc.p10_sips[o.ord] AS p10_sip,
        mfc.p25_sips[o.ord] AS p25_sip,
        mfc.p75_sips[o.ord] AS p75_sip,
        mfc.p90_sips[o.ord] AS p90_sip,
        mfc.p98_sips[o.ord] AS p98_sip
    FROM matched_forecasts AS mfc
        CROSS JOIN LATERAL UNNEST(mfc.p50_sips) WITH ORDINALITY AS o (p50_sip, ord)
)
/* Column order here is load-bearing: StreamForecastData bypasses sqlc and scans these
 * positionally, so reordering silently corrupts the stream. */
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
        LIMIT 1 --noqa
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
 */
WITH target_source AS (
    SELECT
        sv.is_composite,
        sv.source_geometry_uuids,
        sv.source_weights
    FROM loc.sources_mv AS sv
    WHERE sv.geometry_uuid = $1
        AND sv.source_type_id = $2
    LIMIT 1
),
target_components AS MATERIALIZED (
    -- Direct predictions on the target location itself take priority when present
    SELECT
        TRUE AS is_direct,
        $1::UUID AS component_geometry_uuid,
        1.0::DOUBLE PRECISION AS weight,
        1 AS component_count
    FROM target_source
    UNION ALL
    -- Otherwise fall back to the composite component geometry UUIDs and weights
    SELECT
        FALSE AS is_direct,
        UNNEST(src.source_geometry_uuids) AS component_geometry_uuid,
        UNNEST(src.source_weights) AS weight,
        CARDINALITY(src.source_geometry_uuids) AS component_count
    FROM target_source AS src
    WHERE src.is_composite
),
distinct_components AS (
    SELECT DISTINCT component_geometry_uuid
    FROM target_components
),
component_sources AS MATERIALIZED (
    -- Fetch each component's capacity and location metadata once
    SELECT
        cs.geometry_uuid,
        cs.capacity_watts,
        cs.latitude,
        cs.longitude,
        cs.geometry_name,
        cs.sys_period
    FROM distinct_components AS dc
        INNER JOIN loc.sources_mv AS cs
        ON cs.geometry_uuid = dc.component_geometry_uuid
            AND cs.source_type_id = $2
            AND cs.sys_period && TSRANGE(
                sqlc.arg(start_timestamp_utc)::TIMESTAMP,
                sqlc.arg(end_timestamp_utc)::TIMESTAMP,
                '[]'
            )
),
allowed_forecasts AS (
    -- Fetch all forecasts for the components that are within the requested time window and horizon
    SELECT
        f.forecast_uuid,
        f.geometry_uuid,
        f.source_type_id,
        f.created_at_utc,
        f.metadata,
        f.value_resolution_mins,
        f.init_time_utc,
        f.p02_sips,
        f.p10_sips,
        f.p25_sips,
        f.p50_sips,
        f.p75_sips,
        f.p90_sips,
        f.p98_sips,
        LOWER(f.target_period) AS first_target_utc
    FROM distinct_components AS dc
        INNER JOIN pred.forecasts AS f
        ON f.geometry_uuid = dc.component_geometry_uuid
    WHERE f.source_type_id = $2
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
            sqlc.arg(end_timestamp_utc)::TIMESTAMP, '[]'
        )
),
sliced AS MATERIALIZED (
    /* Convert the target-time window and minimum horizon into an array index range.
     * This means that a forecast with a window that only partially overlaps the requested
     * window will only be partially expanded.
     *
     * MATERIALIZED pins lo and hi to one evaluation per forecast. Inlined, the planner
     * substitutes their NUMERIC-heavy definitions into every expression downstream that
     * derives from them - target_time_utc, horizon_mins, and both sort keys - and recomputes
     * them per expanded row rather than per forecast. Measured at ~2x on the read path. */
    SELECT
        af.*,
        GREATEST(1, CEIL(EXTRACT(EPOCH FROM (GREATEST(
            sqlc.arg(start_timestamp_utc)::TIMESTAMP,
            af.init_time_utc + MAKE_INTERVAL(mins => sqlc.arg(horizon_mins)::INTEGER)
        ) - af.first_target_utc)) / 60.0 / af.value_resolution_mins)::INTEGER + 1) AS lo,
        LEAST(ARRAY_LENGTH(af.p50_sips, 1), FLOOR(EXTRACT(EPOCH FROM (
            sqlc.arg(end_timestamp_utc)::TIMESTAMP - af.first_target_utc
        )) / 60.0 / af.value_resolution_mins)::INTEGER + 1) AS hi
    FROM allowed_forecasts AS af
),
expanded AS (
    /* Expand the sliced arrays into rows, with each row representing a single
     * target time and its associated predicted values.
     * UNNEST renumbers from 1 regardless of the slice's lower bound, hence the
     * s.lo + o.ord - 1 indexing into the sibling p-level arrays. */
    SELECT
        s.forecast_uuid,
        s.init_time_utc,
        s.created_at_utc,
        s.metadata,
        s.geometry_uuid,
        s.source_type_id,
        o.p50_sip::SMALLINT AS p50_sip,
        s.p02_sips[s.lo + o.ord - 1]::SMALLINT AS p02_sip,
        s.p10_sips[s.lo + o.ord - 1]::SMALLINT AS p10_sip,
        s.p25_sips[s.lo + o.ord - 1]::SMALLINT AS p25_sip,
        s.p75_sips[s.lo + o.ord - 1]::SMALLINT AS p75_sip,
        s.p90_sips[s.lo + o.ord - 1]::SMALLINT AS p90_sip,
        s.p98_sips[s.lo + o.ord - 1]::SMALLINT AS p98_sip,
        (s.first_target_utc + MAKE_INTERVAL(
            mins =>
            ((s.lo + o.ord - 2) * s.value_resolution_mins)::INTEGER
        ))::TIMESTAMP
            AS target_time_utc,
        (EXTRACT(EPOCH FROM (
            s.first_target_utc
            + MAKE_INTERVAL(mins => ((s.lo + o.ord - 2) * s.value_resolution_mins)::INTEGER)
            - s.init_time_utc
        )) / 60)::SMALLINT AS horizon_mins
    FROM sliced AS s
        CROSS JOIN
            LATERAL UNNEST(s.p50_sips[s.lo:s.hi])
            WITH ORDINALITY AS o (p50_sip, ord)
    WHERE s.hi >= s.lo
),
winning_predictions AS (
    /* ordering by descending forecast_uuid means the lowest horizons are selected first,
     * since init times are encoded within it.
     * Ordering by target_time_utc ASC first keeps the stream pre-sorted for the downstream
     * GROUP BY and final ORDER BY. */
    SELECT DISTINCT ON (target_time_utc, geometry_uuid) *
    FROM expanded
    ORDER BY target_time_utc ASC, geometry_uuid ASC, forecast_uuid DESC
),
aggregated AS (
    /* Aggregate the winning predictions for each target time across all components via weighted sum
     * There is less oging on here than it looks, it's just repeated logic for each p-level.
     * The CASE statements avoid dividing by zero when components are missing a p-level. */
    SELECT
        tc.is_direct,
        MIN(wp.horizon_mins)::SMALLINT AS horizon_mins,
        CASE
            WHEN BOOL_AND(wp.p02_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(tc.weight * comp_sv.capacity_watts * wp.p02_sip)
                    / NULLIF(SUM(tc.weight * comp_sv.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p02_sip,
        CASE
            WHEN BOOL_AND(wp.p25_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(tc.weight * comp_sv.capacity_watts * wp.p25_sip)
                    / NULLIF(SUM(tc.weight * comp_sv.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p25_sip,
        CASE
            WHEN BOOL_AND(wp.p10_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(tc.weight * comp_sv.capacity_watts * wp.p10_sip)
                    / NULLIF(SUM(tc.weight * comp_sv.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p10_sip,
        COALESCE(ROUND(
            SUM(tc.weight * comp_sv.capacity_watts * wp.p50_sip)
            / NULLIF(SUM(tc.weight * comp_sv.capacity_watts), 0.0)
        ), 0)::SMALLINT AS p50_sip,
        CASE
            WHEN BOOL_AND(wp.p75_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(tc.weight * comp_sv.capacity_watts * wp.p75_sip)
                    / NULLIF(SUM(tc.weight * comp_sv.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p75_sip,
        CASE
            WHEN BOOL_AND(wp.p90_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(tc.weight * comp_sv.capacity_watts * wp.p90_sip)
                    / NULLIF(SUM(tc.weight * comp_sv.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p90_sip,
        CASE
            WHEN BOOL_AND(wp.p98_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(tc.weight * comp_sv.capacity_watts * wp.p98_sip)
                    / NULLIF(SUM(tc.weight * comp_sv.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p98_sip,
        wp.target_time_utc,
        (ARRAY_AGG(wp.metadata) FILTER (WHERE tc.component_count = 1))[1]::JSONB AS metadata,
        MAX(wp.init_time_utc)::TIMESTAMP AS init_time_utc,
        MAX(wp.created_at_utc)::TIMESTAMP AS created_at_utc
    FROM winning_predictions AS wp
        INNER JOIN target_components AS tc
            ON tc.component_geometry_uuid = wp.geometry_uuid
        INNER JOIN component_sources AS comp_sv
            ON comp_sv.geometry_uuid = wp.geometry_uuid
            AND comp_sv.sys_period @> wp.target_time_utc
    GROUP BY wp.target_time_utc, tc.is_direct, tc.component_count
    HAVING COUNT(*) = tc.component_count
),
winning AS (
    SELECT DISTINCT ON (a.target_time_utc) *
    FROM aggregated AS a
    ORDER BY a.target_time_utc ASC, a.init_time_utc DESC, a.is_direct DESC
)
SELECT
    w.horizon_mins,
    w.p02_sip,
    w.p25_sip,
    w.p10_sip,
    w.p50_sip,
    w.p75_sip,
    w.p90_sip,
    w.p98_sip,
    w.target_time_utc,
    w.metadata,
    w.init_time_utc,
    w.created_at_utc,
    sv.capacity_watts,
    sv.latitude,
    sv.longitude,
    sv.geometry_name
FROM winning AS w
    INNER JOIN loc.sources_mv AS sv
        ON sv.geometry_uuid = $1
        AND sv.source_type_id = $2
        AND sv.sys_period @> w.target_time_utc
ORDER BY w.target_time_utc ASC;

-- name: ListPredictionsAtTimeForLocations :many
/* ListPredictionsAtTimeForLocations retrieves the latest prediction for a single target time
 * across many locations - the map query.
 *
 * PostgreSQL returns NULL on an out of bounds array index, so ARRAY_LENGTH guards the
 * subscript: if target_period and the array length ever disagree, the row is omitted rather
 * than silently returning NULL values.
 */
WITH requested_sources AS MATERIALIZED (
    SELECT
        sv.geometry_uuid,
        sv.source_type_id,
        sv.capacity_watts,
        sv.latitude,
        sv.longitude,
        sv.geometry_name,
        sv.is_composite,
        sv.source_geometry_uuids,
        sv.source_weights,
        CARDINALITY(sv.source_geometry_uuids) AS component_count
    FROM loc.sources_mv AS sv
    WHERE sv.geometry_uuid = ANY(sqlc.arg(geometry_uuids)::UUID[])
        AND sv.source_type_id = $1
        AND sv.sys_period @> sqlc.arg(target_timestamp_utc)::TIMESTAMP
),
location_components AS (
    -- Direct prediction candidate on each requested location itself
    SELECT
        rs.geometry_uuid AS target_geometry_uuid,
        TRUE AS is_direct,
        1 AS component_count,
        rs.geometry_uuid AS component_geometry_uuid,
        1.0::DOUBLE PRECISION AS weight
    FROM requested_sources AS rs
    UNION ALL
    -- Composite component candidates when the requested location is composite
    SELECT
        rs.geometry_uuid AS target_geometry_uuid,
        FALSE AS is_direct,
        rs.component_count,
        UNNEST(rs.source_geometry_uuids) AS component_geometry_uuid,
        UNNEST(rs.source_weights) AS weight
    FROM requested_sources AS rs
    WHERE rs.is_composite
),
distinct_components AS (
    -- There may be multiple instances of the same component across seperate locations
    SELECT DISTINCT component_geometry_uuid
    FROM location_components
),
latest_allowed_forecast_per_location AS (
    -- Find the single latest foreacst value and capacity at target timestamp for each component
    SELECT
        lf.forecast_uuid,
        dc.component_geometry_uuid AS geometry_uuid,
        lf.source_type_id,
        lf.created_at_utc,
        lf.metadata,
        lf.init_time_utc,
        lf.value_resolution_mins,
        lf.p02_sips,
        lf.p10_sips,
        lf.p25_sips,
        lf.p50_sips,
        lf.p75_sips,
        lf.p90_sips,
        lf.p98_sips,
        LOWER(lf.target_period) AS first_target_utc
    FROM distinct_components AS dc
        CROSS JOIN LATERAL (
            SELECT
                f.forecast_uuid,
                f.source_type_id,
                f.created_at_utc,
                f.metadata,
                f.init_time_utc,
                f.value_resolution_mins,
                f.target_period,
                f.p02_sips,
                f.p10_sips,
                f.p25_sips,
                f.p50_sips,
                f.p75_sips,
                f.p90_sips,
                f.p98_sips
            FROM pred.forecasts AS f
            WHERE f.geometry_uuid = dc.component_geometry_uuid
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
     * so i = (target - first_target) / resolution + 1. */
    SELECT
        laf.*,
        (EXTRACT(EPOCH FROM (
            sqlc.arg(target_timestamp_utc)::TIMESTAMP - laf.first_target_utc
        )) / 60 / laf.value_resolution_mins)::INTEGER + 1 AS idx,
        (EXTRACT(EPOCH FROM (
            sqlc.arg(target_timestamp_utc)::TIMESTAMP - laf.init_time_utc
        )) / 60)::SMALLINT AS horizon_mins
    FROM latest_allowed_forecast_per_location AS laf
),
picked AS MATERIALIZED (
    SELECT
        i.forecast_uuid,
        i.geometry_uuid,
        i.source_type_id,
        i.horizon_mins,
        i.created_at_utc,
        i.init_time_utc,
        i.metadata,
        comp_sv.capacity_watts,
        i.p02_sips[i.idx] AS p02_sip,
        i.p10_sips[i.idx] AS p10_sip,
        i.p25_sips[i.idx] AS p25_sip,
        i.p50_sips[i.idx] AS p50_sip,
        i.p75_sips[i.idx] AS p75_sip,
        i.p90_sips[i.idx] AS p90_sip,
        i.p98_sips[i.idx] AS p98_sip
    FROM indexed AS i
        INNER JOIN loc.sources_mv AS comp_sv
            ON comp_sv.geometry_uuid = i.geometry_uuid
            AND comp_sv.source_type_id = $1
            AND comp_sv.sys_period @> sqlc.arg(target_timestamp_utc)::TIMESTAMP
    WHERE i.idx BETWEEN 1 AND ARRAY_LENGTH(i.p50_sips, 1)
        /* Rejects a target time that falls between array slots. The row model got this for free
         * from the join on horizon_mins; without it a misaligned request silently returns a
         * neighbouring value instead of no row. */
        AND MOD(
            (EXTRACT(EPOCH FROM (sqlc.arg(target_timestamp_utc)::TIMESTAMP - i.first_target_utc)) / 60)::NUMERIC,
            i.value_resolution_mins::NUMERIC
        ) = 0
),
aggregated AS (
    /* Aggregate the winning predictions for each target time across all components via weighted sum
     * Again, the CASE statements avoid dividing by zero when components are missing a p-level. */
    SELECT
        (ARRAY_AGG(p.forecast_uuid) FILTER (WHERE lc.component_count = 1))[1]::UUID AS forecast_uuid,
        lc.target_geometry_uuid,
        lc.is_direct,
        MIN(p.horizon_mins)::SMALLINT AS horizon_mins,
        MAX(p.created_at_utc)::TIMESTAMP AS created_at_utc,
        MAX(p.init_time_utc)::TIMESTAMP AS init_time_utc,
        (ARRAY_AGG(p.metadata) FILTER (WHERE lc.component_count = 1))[1]::JSONB AS metadata,
        CASE
            WHEN BOOL_AND(p.p02_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(lc.weight * p.capacity_watts * p.p02_sip)
                    / NULLIF(SUM(lc.weight * p.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p02_sip,
        CASE
            WHEN BOOL_AND(p.p10_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(lc.weight * p.capacity_watts * p.p10_sip)
                    / NULLIF(SUM(lc.weight * p.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p10_sip,
        CASE
            WHEN BOOL_AND(p.p25_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(lc.weight * p.capacity_watts * p.p25_sip)
                    / NULLIF(SUM(lc.weight * p.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p25_sip,
        COALESCE(ROUND(
            SUM(lc.weight * p.capacity_watts * p.p50_sip)
            / NULLIF(SUM(lc.weight * p.capacity_watts), 0.0)
        ), 0)::SMALLINT AS p50_sip,
        CASE
            WHEN BOOL_AND(p.p75_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(lc.weight * p.capacity_watts * p.p75_sip)
                    / NULLIF(SUM(lc.weight * p.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p75_sip,
        CASE
            WHEN BOOL_AND(p.p90_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(lc.weight * p.capacity_watts * p.p90_sip)
                    / NULLIF(SUM(lc.weight * p.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p90_sip,
        CASE
            WHEN BOOL_AND(p.p98_sip IS NOT NULL)
                THEN COALESCE(ROUND(
                    SUM(lc.weight * p.capacity_watts * p.p98_sip)
                    / NULLIF(SUM(lc.weight * p.capacity_watts), 0.0)
                ), 0)::SMALLINT
            ELSE NULL::SMALLINT
        END AS p98_sip,
        sqlc.arg(target_timestamp_utc)::TIMESTAMP AS target_time_utc
    FROM location_components AS lc
        INNER JOIN picked AS p
            ON p.geometry_uuid = lc.component_geometry_uuid
    GROUP BY lc.target_geometry_uuid, lc.is_direct, lc.component_count
    HAVING COUNT(*) = lc.component_count
),
winning AS (
    SELECT DISTINCT ON (a.target_geometry_uuid) *
    FROM aggregated AS a
    ORDER BY a.target_geometry_uuid ASC, a.init_time_utc DESC, a.is_direct DESC
)
SELECT
    w.forecast_uuid,
    rs.geometry_uuid,
    rs.source_type_id,
    w.horizon_mins,
    w.created_at_utc,
    w.init_time_utc,
    rs.capacity_watts,
    rs.latitude,
    rs.longitude,
    rs.geometry_name,
    w.metadata,
    w.p02_sip,
    w.p10_sip,
    w.p25_sip,
    w.p50_sip,
    w.p75_sip,
    w.p90_sip,
    w.p98_sip,
    w.target_time_utc
FROM winning AS w
    INNER JOIN requested_sources AS rs
        ON rs.geometry_uuid = w.target_geometry_uuid;

-- name: GetWeekAverageDeltasForLocations :many
/* GetWeekAverageDeltasForLocations retrieves the average deltas between predicted and observed
 * generation values for a given source type, forecaster, and observer, across a week of
 * forecasts made with the same init time. The pivot timestamp determines the week and init time
 * of interest. Results are grouped by location and horizon.
 */
WITH relevant_forecasts AS (
    SELECT
        f.forecast_uuid,
        f.source_type_id,
        f.geometry_uuid,
        f.forecaster_id,
        f.init_time_utc,
        f.value_resolution_mins,
        f.p50_sips,
        LOWER(f.target_period) AS first_target_utc
    FROM pred.forecasts AS f
    WHERE f.geometry_uuid = $4
        AND f.source_type_id = $1
        AND f.forecaster_id = $2
        AND f.forecast_uuid >= UUIDV7_BOUNDARY(sqlc.arg(pivot_timestamp)::TIMESTAMP - INTERVAL '8 days')
        AND f.forecast_uuid < UUIDV7_BOUNDARY(sqlc.arg(pivot_timestamp)::TIMESTAMP + INTERVAL '1 millisecond')
        AND f.init_time_utc::TIME = sqlc.arg(pivot_timestamp)::TIMESTAMP::TIME
),
relevant_predicted_values AS MATERIALIZED (
    SELECT
        rf.geometry_uuid,
        rf.source_type_id,
        o.p50_sip,
        (
            EXTRACT(EPOCH FROM (rf.first_target_utc - rf.init_time_utc)) / 60
            + (o.ord - 1) * rf.value_resolution_mins
        )::SMALLINT AS horizon_mins,
        (rf.first_target_utc + MAKE_INTERVAL(
            mins =>
            ((o.ord - 1) * rf.value_resolution_mins)::INTEGER
        ))::TIMESTAMP AS target_time_utc
    FROM relevant_forecasts AS rf
        CROSS JOIN LATERAL UNNEST(rf.p50_sips) WITH ORDINALITY AS o (p50_sip, ord)
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

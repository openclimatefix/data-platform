-- name: CreateObserver :one
INSERT INTO obs.observers (observer_name) VALUES (LOWER(sqlc.arg(observer_name)::TEXT)) RETURNING
    observer_uuid, observer_name;

-- name: GetObserversByFilters :many
SELECT
    observer_uuid,
    observer_name
FROM obs.observers
WHERE (
    ARRAY_LENGTH(sqlc.arg(observer_names)::TEXT[], 1) IS NULL
    OR observer_name = ANY(sqlc.arg(observer_names)::TEXT[])
);

-- name: GetObserverByName :one
SELECT
    o.observer_uuid,
    o.observer_name,
    UUIDV7_EXTRACT_TIMESTAMP(o.observer_uuid)::TIMESTAMP AS created_at_utc
FROM obs.observers AS o
WHERE o.observer_name = $1;

-- name: CreateObservations :copyfrom
/* CreateObservations inserts a batch of observations using postgres COPY protocol,
 * making it the fastest way to perform large inserts of observations.
 * Input yields are expected as 16-bit integers, with 0 representing 0%
 * and 30000 representing 100% of capacity.
 */
INSERT INTO obs.observed_generation_values (
    geometry_uuid, source_type_id, observer_uuid, observation_timestamp_utc, value_sip, created_timestamp_utc
) VALUES (
    $1, $2, $3, $4, $5, $6
);

-- name: CreateObservationsBatch :batchone
/* CreateObservationsBatch inserts observations in batch mode.
 * Input yield is given as watts, so we join the sources materialized view
 * to determine the capacity at the given timestamp.
 */
INSERT INTO obs.observed_generation_values (
    geometry_uuid,
    source_type_id,
    observer_uuid,
    observation_timestamp_utc,
    value_sip,
    created_timestamp_utc
)
SELECT
    mv.geometry_uuid,
    mv.source_type_id,
    sqlc.arg(observer_uuid)::UUID,
    sqlc.arg(observation_timestamp_utc)::TIMESTAMP,
    ((sqlc.arg(value_watts)::BIGINT::DOUBLE PRECISION / mv.capacity_watts) * 30000.0)::SMALLINT AS calculated_value_sip,
    sqlc.arg(created_timestamp_utc)::TIMESTAMP
FROM loc.sources_mv AS mv
WHERE mv.geometry_uuid = sqlc.arg(geometry_uuid)::UUID
    AND mv.source_type_id = sqlc.arg(source_type_id)::SMALLINT
    AND mv.sys_period @> sqlc.arg(observation_timestamp_utc)::TIMESTAMP
ON CONFLICT (geometry_uuid, source_type_id, observer_uuid, observation_timestamp_utc)
DO NOTHING
RETURNING *;

-- name: GetObservationsBetween :many
/* GetObservationsBetween gets observations between two timestamps
 * and returns their values as 16-bit integers, with 0 representing 0%
 * and 30000 representing 100% of capacity.
 */
WITH target_source AS (
    SELECT
        sv.is_composite,
        sv.source_geometry_uuids,
        sv.source_weights
    FROM loc.sources_mv AS sv
    WHERE sv.geometry_uuid = $1
        AND sv.source_type_id = $2
    ORDER BY sv.sys_period DESC
    LIMIT 1
),
target_components AS MATERIALIZED (
    -- Direct observations on the target location itself take priority when present
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
    -- Get the capacities of each component source
    SELECT
        cs.geometry_uuid,
        cs.source_type_id,
        cs.capacity_watts,
        cs.sys_period
    FROM distinct_components AS dc
        INNER JOIN loc.sources_mv AS cs
        ON dc.component_geometry_uuid = cs.geometry_uuid
            AND cs.source_type_id = $2
            AND cs.sys_period && TSRANGE(
                sqlc.arg(start_time_utc)::TIMESTAMP,
                sqlc.arg(end_time_utc)::TIMESTAMP,
                '[]'
            )
),
component_observations AS MATERIALIZED (
    -- Fetch observations once per distinct component before joining capacity slices
    SELECT
        og.geometry_uuid,
        og.observation_timestamp_utc,
        og.value_sip,
        og.created_timestamp_utc
    FROM distinct_components AS dc
        INNER JOIN obs.observed_generation_values AS og
        ON dc.component_geometry_uuid = og.geometry_uuid
            AND og.source_type_id = $2
            AND og.observer_uuid = $3
            AND og.observation_timestamp_utc
            BETWEEN sqlc.arg(start_time_utc)::TIMESTAMP AND sqlc.arg(end_time_utc)::TIMESTAMP
            AND og.created_timestamp_utc <= sqlc.arg(pivot_time_utc)::TIMESTAMP
),
aggregated AS (
    -- Combine observations for all the components, grouped by timestamp, and weighted-average them
    SELECT
        og.observation_timestamp_utc,
        tc.is_direct,
        COALESCE(ROUND(
            SUM(tc.weight * comp_sh.capacity_watts * og.value_sip)
            / NULLIF(SUM(tc.weight * comp_sh.capacity_watts), 0.0)
        ), 0)::SMALLINT AS value_sip,
        MAX(og.created_timestamp_utc)::TIMESTAMP AS created_timestamp_utc
    FROM component_observations AS og
        INNER JOIN target_components AS tc
        ON og.geometry_uuid = tc.component_geometry_uuid
        INNER JOIN component_sources AS comp_sh
        ON og.geometry_uuid = comp_sh.geometry_uuid
            AND comp_sh.sys_period @> og.observation_timestamp_utc
    GROUP BY og.observation_timestamp_utc, tc.is_direct, tc.component_count
    -- Drop any timestamps that don't have the full complement of component observations
    HAVING COUNT(*) = tc.component_count
),
winning AS (
    SELECT DISTINCT ON (a.observation_timestamp_utc)
        a.observation_timestamp_utc,
        a.value_sip,
        a.created_timestamp_utc
    FROM aggregated AS a
    ORDER BY a.observation_timestamp_utc ASC, a.is_direct DESC
)
SELECT
    sh.geometry_uuid,
    sh.source_type_id,
    w.observation_timestamp_utc,
    sh.capacity_watts,
    w.value_sip,
    w.created_timestamp_utc
FROM winning AS w
    INNER JOIN loc.sources_mv AS sh
    ON sh.geometry_uuid = $1
        AND sh.source_type_id = $2
        AND sh.sys_period @> w.observation_timestamp_utc
ORDER BY w.observation_timestamp_utc ASC;

-- name: GetLatestObservations :many
/* GetLatestObservations gets the latest observations for a given location set, source type, and
 * observer. The value is returned as a 16-bit integer, with 0 representing 0%
 * and 30000 representing 100% of capacity.
 *
 * It uses lateral joins to perform a reverse index scan for efficiency.
 */
WITH target_locations AS (
    SELECT UNNEST(sqlc.arg(geometry_uuids)::UUID[]) AS geometry_uuid
),
target_observer AS (
    SELECT observer_uuid
    FROM obs.observers
    WHERE observer_name = LOWER(sqlc.arg(observer_name)::TEXT)
),
target_sources AS (
    SELECT
        tl.geometry_uuid,
        src.is_composite,
        src.source_geometry_uuids,
        src.source_weights
    FROM target_locations AS tl
        CROSS JOIN
            LATERAL (
                SELECT
                    sv.is_composite,
                    sv.source_geometry_uuids,
                    sv.source_weights
                FROM loc.sources_mv AS sv
                WHERE sv.geometry_uuid = tl.geometry_uuid
                    AND sv.source_type_id = sqlc.arg(source_type_id)::SMALLINT
                ORDER BY sv.sys_period DESC
                LIMIT 1
            ) AS src
),
location_components AS MATERIALIZED (
    -- Direct observation candidate on the target location itself
    SELECT
        ts.geometry_uuid,
        TRUE AS is_direct,
        ts.geometry_uuid AS component_geometry_uuid,
        1.0::DOUBLE PRECISION AS weight,
        1 AS component_count
    FROM target_sources AS ts
    UNION ALL
    -- Composite component candidates when the location is composite
    SELECT
        ts.geometry_uuid,
        FALSE AS is_direct,
        UNNEST(ts.source_geometry_uuids) AS component_geometry_uuid,
        UNNEST(ts.source_weights) AS weight,
        CARDINALITY(ts.source_geometry_uuids) AS component_count
    FROM target_sources AS ts
    WHERE ts.is_composite
),
distinct_components AS (
    SELECT DISTINCT component_geometry_uuid
    FROM location_components
),
component_latest AS MATERIALIZED (
    -- For each component get the latest observation
    SELECT
        dc.component_geometry_uuid,
        lo.observation_timestamp_utc,
        lo.value_sip
    FROM distinct_components AS dc
        CROSS JOIN target_observer AS tobs
        -- Perform a reverse index scan for speed
        CROSS JOIN
            LATERAL (
                SELECT
                    og.observation_timestamp_utc,
                    og.value_sip
                FROM obs.observed_generation_values AS og
                WHERE og.geometry_uuid = dc.component_geometry_uuid
                    AND og.source_type_id = sqlc.arg(source_type_id)::SMALLINT
                    AND og.observer_uuid = tobs.observer_uuid
                    AND og.observation_timestamp_utc
                    >= sqlc.arg(pivot_time_utc)::TIMESTAMP - INTERVAL '7 days'
                    AND og.observation_timestamp_utc <= sqlc.arg(pivot_time_utc)::TIMESTAMP
                    AND og.created_timestamp_utc <= sqlc.arg(pivot_time_utc)::TIMESTAMP
                ORDER BY og.observation_timestamp_utc DESC
                LIMIT 1
            ) AS lo
),
location_tip AS (
    -- For each target location candidate group, get the earliest and latest observation timestamps across components
    SELECT
        lc.geometry_uuid,
        lc.is_direct,
        lc.component_count,
        MIN(cl.observation_timestamp_utc)::TIMESTAMP AS min_tip_utc,
        MAX(cl.observation_timestamp_utc)::TIMESTAMP AS max_tip_utc
    FROM location_components AS lc
        INNER JOIN component_latest AS cl
        ON lc.component_geometry_uuid = cl.component_geometry_uuid
    GROUP BY lc.geometry_uuid, lc.is_direct, lc.component_count
    HAVING COUNT(*) = lc.component_count
),
resolved_components AS (
    -- If all components have the same latest timestamp, return them directly
    SELECT
        lt.geometry_uuid,
        lt.is_direct,
        lc.component_geometry_uuid,
        lc.weight,
        cl.observation_timestamp_utc,
        cl.value_sip
    FROM location_tip AS lt
        INNER JOIN location_components AS lc
        ON lt.geometry_uuid = lc.geometry_uuid
            AND lt.is_direct = lc.is_direct
        INNER JOIN component_latest AS cl
        ON lc.component_geometry_uuid = cl.component_geometry_uuid
    WHERE lt.min_tip_utc = lt.max_tip_utc
    UNION ALL
    -- If components have different latest timestamps, find the latest timestamp that has all components
    SELECT
        lt.geometry_uuid,
        lt.is_direct,
        UNNEST(shared.comp_uuids) AS component_geometry_uuid,
        UNNEST(shared.comp_weights) AS weight,
        shared.observation_timestamp_utc,
        UNNEST(shared.comp_sips) AS value_sip
    FROM location_tip AS lt
        CROSS JOIN target_observer AS tobs
        -- ...again, via a reverse index scan
        CROSS JOIN
            LATERAL (
                SELECT
                    og.observation_timestamp_utc,
                    ARRAY_AGG(lc.component_geometry_uuid) AS comp_uuids,
                    ARRAY_AGG(lc.weight) AS comp_weights,
                    ARRAY_AGG(og.value_sip) AS comp_sips
                FROM location_components AS lc
                    INNER JOIN obs.observed_generation_values AS og
                    ON lc.component_geometry_uuid = og.geometry_uuid
                        AND og.source_type_id = sqlc.arg(source_type_id)::SMALLINT
                        AND tobs.observer_uuid = og.observer_uuid
                        AND og.observation_timestamp_utc
                        >= sqlc.arg(pivot_time_utc)::TIMESTAMP - INTERVAL '7 days'
                        AND lt.min_tip_utc >= og.observation_timestamp_utc
                        AND og.observation_timestamp_utc <= sqlc.arg(pivot_time_utc)::TIMESTAMP
                        AND og.created_timestamp_utc <= sqlc.arg(pivot_time_utc)::TIMESTAMP
                WHERE lc.geometry_uuid = lt.geometry_uuid
                    AND lc.is_direct = lt.is_direct
                GROUP BY og.observation_timestamp_utc, lt.component_count
                HAVING COUNT(*) = lt.component_count
                ORDER BY og.observation_timestamp_utc DESC
                LIMIT 1
            ) AS shared
    WHERE lt.min_tip_utc < lt.max_tip_utc
),
aggregated_latest AS (
    -- Combine component values into a single weighted average at the winning timestamp
    SELECT
        rc.geometry_uuid,
        rc.is_direct,
        rc.observation_timestamp_utc,
        COALESCE(ROUND(
            SUM(rc.weight * comp_sh.capacity_watts * rc.value_sip)
            / NULLIF(SUM(rc.weight * comp_sh.capacity_watts), 0.0)
        ), 0)::SMALLINT AS value_sip
    FROM resolved_components AS rc
        INNER JOIN loc.sources_mv AS comp_sh
        ON rc.component_geometry_uuid = comp_sh.geometry_uuid
            AND comp_sh.source_type_id = sqlc.arg(source_type_id)::SMALLINT
            AND comp_sh.sys_period @> rc.observation_timestamp_utc
    GROUP BY rc.geometry_uuid, rc.is_direct, rc.observation_timestamp_utc
),
winning_latest AS (
    SELECT DISTINCT ON (al.geometry_uuid)
        al.geometry_uuid,
        al.observation_timestamp_utc,
        al.value_sip
    FROM aggregated_latest AS al
    ORDER BY al.geometry_uuid ASC, al.observation_timestamp_utc DESC, al.is_direct DESC
)
SELECT
    wl.geometry_uuid::UUID AS geometry_uuid, -- SQLC complains without this
    wl.observation_timestamp_utc,
    wl.value_sip,
    sh.capacity_limit_sip,
    sh.capacity_watts,
    sqlc.arg(source_type_id)::SMALLINT AS source_type_id
FROM winning_latest AS wl
    INNER JOIN loc.sources_mv AS sh
    ON wl.geometry_uuid = sh.geometry_uuid
        AND sh.source_type_id = sqlc.arg(source_type_id)::SMALLINT
        AND sh.sys_period @> wl.observation_timestamp_utc;

-- name: ListObservationsAtTimeForLocations :many
/* ListObservationsAtTimeForLocations retrieves observed generation values as percentages
 * of capacity for a specific time.
 * This is useful for comparing observations across multiple locations.
 * Observed values are 16-bit integers, with 0 representing 0% and 30000 representing 100% of capacity.
 */
WITH requested_sources AS MATERIALIZED (
    SELECT
        sh.geometry_uuid,
        sh.source_type_id,
        sh.capacity_watts,
        sh.latitude,
        sh.longitude,
        sh.geometry_name,
        sh.is_composite,
        sh.source_geometry_uuids,
        sh.source_weights,
        CARDINALITY(sh.source_geometry_uuids) AS component_count
    FROM loc.sources_mv AS sh
    WHERE sh.geometry_uuid = ANY(sqlc.arg(geometry_uuids)::UUID[])
        AND sh.source_type_id = $1
        AND sh.sys_period @> sqlc.arg(target_timestamp_utc)::TIMESTAMP
),
location_components AS (
    -- Direct observation candidate on each requested location itself
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
    SELECT DISTINCT component_geometry_uuid
    FROM location_components
),
component_observations AS MATERIALIZED (
    -- Fetch observations and capacity once per distinct component
    SELECT
        dc.component_geometry_uuid,
        og.observation_timestamp_utc,
        og.value_sip,
        og.created_timestamp_utc,
        comp_sh.capacity_watts
    FROM distinct_components AS dc
        INNER JOIN obs.observed_generation_values AS og
        ON dc.component_geometry_uuid = og.geometry_uuid
            AND og.source_type_id = $1
            AND og.observer_uuid = $2
            AND og.observation_timestamp_utc = sqlc.arg(target_timestamp_utc)::TIMESTAMP
        INNER JOIN loc.sources_mv AS comp_sh
        ON dc.component_geometry_uuid = comp_sh.geometry_uuid
            AND comp_sh.source_type_id = $1
            AND comp_sh.sys_period @> sqlc.arg(target_timestamp_utc)::TIMESTAMP
),
aggregated AS (
    -- Aggregate the component observations into a single weighted average for each requested source
    SELECT
        lc.target_geometry_uuid,
        lc.is_direct,
        co.observation_timestamp_utc,
        COALESCE(ROUND(
            SUM(lc.weight * co.capacity_watts * co.value_sip)
            / NULLIF(SUM(lc.weight * co.capacity_watts), 0.0)
        ), 0)::SMALLINT AS value_sip,
        MAX(co.created_timestamp_utc)::TIMESTAMP AS created_timestamp_utc
    FROM location_components AS lc
        INNER JOIN component_observations AS co
        ON lc.component_geometry_uuid = co.component_geometry_uuid
    GROUP BY lc.target_geometry_uuid, lc.is_direct, lc.component_count, co.observation_timestamp_utc
    -- Drop any timestamps that don't have the full complement of component observations
    HAVING COUNT(*) = lc.component_count
),
winning AS (
    SELECT DISTINCT ON (a.target_geometry_uuid)
        a.target_geometry_uuid,
        a.observation_timestamp_utc,
        a.value_sip,
        a.created_timestamp_utc
    FROM aggregated AS a
    ORDER BY a.target_geometry_uuid ASC, a.is_direct DESC
)
SELECT
    rs.geometry_uuid,
    rs.source_type_id,
    w.observation_timestamp_utc,
    rs.capacity_watts,
    rs.latitude,
    rs.longitude,
    rs.geometry_name,
    w.value_sip,
    w.created_timestamp_utc
FROM winning AS w
    INNER JOIN requested_sources AS rs
    ON w.target_geometry_uuid = rs.geometry_uuid;

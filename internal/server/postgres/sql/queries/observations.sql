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
    geometry_uuid, source_type_id, observer_uuid, observation_timestamp_utc, value_sip
) VALUES (
    $1, $2, $3, $4, $5
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
    value_sip
)
SELECT
    mv.geometry_uuid,
    mv.source_type_id,
    sqlc.arg(observer_uuid)::UUID,
    sqlc.arg(observation_timestamp_utc)::TIMESTAMP,
    ((sqlc.arg(value_watts)::BIGINT::DOUBLE PRECISION / mv.capacity_watts) * 30000.0)::SMALLINT AS calculated_value_sip
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
SELECT
    og.geometry_uuid,
    og.source_type_id,
    og.observation_timestamp_utc,
    og.value_sip,
    sh.capacity_watts
FROM obs.observed_generation_values AS og
    INNER JOIN loc.sources_mv AS sh USING (geometry_uuid, source_type_id)
WHERE
    og.geometry_uuid = $1
    AND og.source_type_id = $2
    AND og.observer_uuid = $3
    AND og.observation_timestamp_utc BETWEEN sqlc.arg(start_time_utc)::TIMESTAMP AND sqlc.arg(end_time_utc)::TIMESTAMP
    AND sh.sys_period @> og.observation_timestamp_utc;

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
)
SELECT
    tl.geometry_uuid::UUID AS geometry_uuid, -- SQLC complains without this
    latest_obs.observation_timestamp_utc,
    latest_obs.value_sip,
    sh.capacity_limit_sip,
    sh.capacity_watts,
    sqlc.arg(source_type_id)::SMALLINT AS source_type_id
FROM target_locations AS tl
    CROSS JOIN target_observer AS tobs
    CROSS JOIN
        LATERAL (
            SELECT
                og.observation_timestamp_utc,
                og.value_sip
            FROM obs.observed_generation_values AS og
            WHERE og.geometry_uuid = tl.geometry_uuid
                AND og.source_type_id = sqlc.arg(source_type_id)::SMALLINT
                AND og.observer_uuid = tobs.observer_uuid
                AND og.observation_timestamp_utc <= sqlc.arg(pivot_time_utc)::TIMESTAMP
            ORDER BY og.observation_timestamp_utc DESC
            LIMIT 1
        ) AS latest_obs
    INNER JOIN loc.sources_mv AS sh
    ON tl.geometry_uuid = sh.geometry_uuid
        AND sh.source_type_id = sqlc.arg(source_type_id)::SMALLINT
        AND sh.sys_period @> latest_obs.observation_timestamp_utc;

-- name: ListObservationsAtTimeForLocations :many
/* ListObservationsAtTimeForLocations retrieves observed generation values as percentages
 * of capacity for a specific time.
 * This is useful for comparing observations across multiple locations.
 * Observed values are 16-bit integers, with 0 representing 0% and 30000 representing 100% of capacity.
 */
SELECT
    og.geometry_uuid,
    og.source_type_id,
    og.observation_timestamp_utc,
    og.value_sip,
    sh.capacity_watts,
    sh.latitude,
    sh.longitude,
    sh.geometry_name
FROM obs.observed_generation_values AS og
    INNER JOIN loc.sources_mv AS sh USING (geometry_uuid, source_type_id)
WHERE
    og.geometry_uuid = ANY(sqlc.arg(geometry_uuids)::UUID[])
    AND og.source_type_id = $1
    AND og.observer_uuid = $2
    AND og.observation_timestamp_utc = sqlc.arg(target_timestamp_utc)::TIMESTAMP
    AND sh.sys_period @> og.observation_timestamp_utc;

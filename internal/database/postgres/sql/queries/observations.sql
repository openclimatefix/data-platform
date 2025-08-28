-- name: CreateObserver :one
INSERT INTO obs.observers (observer_name) VALUES ($1) RETURNING observer_id;

-- name: ListObservers :many
SELECT
    observer_id,
    observer_name
FROM obs.observers;

-- name: GetObserverByName :one
SELECT
    o.observer_id,
    o.observer_name
FROM obs.observers AS o
WHERE o.observer_name = $1;

-- name: CreateObservations :copyfrom
/* CreateObservations inserts a batch of observations using postgres COPY protocol,
 * making it the fastest way to perform large inserts of observations.
 * Input yields are expected as 16-bit integers, with 0 representing 0%
 * and 30000 representing 100% of capacity.
 */
INSERT INTO obs.observed_generation_values (
    location_uuid, source_type_id, observer_id, observation_timestamp_utc, value_sip
) VALUES (
    $1, $2, $3, $4, $5
);

-- name: GetObservationsBetween :many
/* GetObservationsBetween gets observations between two timestamps
 * and returns their values as 16-bit integers, with 0 representing 0%
 * and 30000 representing 100% of capacity.
 * This is faster than converting the values to percentages.
 */
SELECT
    og.location_uuid,
    og.source_type_id,
    og.observation_timestamp_utc,
    og.value_sip,
    COALESCE(
        sh.capacity_limit_sip::real * sh.capacity / 30000.0, sh.capacity::real
    )::real AS effective_capacity,
    sh.capacity_unit_prefix_factor
FROM obs.observed_generation_values AS og
JOIN loc.sources_mv AS sh USING (location_uuid, source_type_id)
WHERE
    og.location_uuid = $1
    AND og.source_type_id = $2
    AND og.observer_id = $3
    AND og.observation_timestamp_utc BETWEEN sqlc.arg(start_time_utc)::timestamp AND sqlc.arg(end_time_utc)::timestamp
    AND sh.sys_period @> og.observation_timestamp_utc;

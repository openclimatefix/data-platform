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

-- name: CreateObservationsAsInt16UsingCopy :copyfrom
/* CreateObservationsCopy inserts a batch of observations using postgres COPY protocol,
 * making it the fastest way to perform large inserts of observations.
 * Input yields are expected as 16-bit integers, with 0 representing 0%
 * and 30000 representing 100% of capacity.
 */
INSERT INTO obs.observed_generation_values (
    location_id, source_type_id, observer_id, observation_time_utc, value_sip
) VALUES (
    $1, $2, $3, $4, $5
);

-- name: GetObservationsAsInt16Between :many
/* GetObservationsAsInt16 gets observations between two timestamps
 * and returns their values as 16-bit integers, with 0 representing 0%
 * and 30000 representing 100% of capacity.
 * This is faster than converting the values to percentages.
 */
SELECT
    og.location_id,
    og.source_type_id,
    og.observation_time_utc,
    og.value_sip
FROM obs.observed_generation_values AS og
WHERE
    og.location_id = $1
    AND og.source_type_id = $2
    AND og.observer_id = $3
    AND og.observation_time_utc BETWEEN sqlc.arg(start_time_utc)::timestamp AND sqlc.arg(end_time_utc)::timestamp;

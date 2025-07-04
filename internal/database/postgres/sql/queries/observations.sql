-- name: CreateObserver :one
INSERT INTO obs.observers (observer_name) VALUES ($1) RETURNING observer_id;

-- name: ListObservers :many
SELECT
    observer_id, observer_name
FROM obs.observers;

-- name: GetObserverByName :one
SELECT
    observer_id, observer_name
FROM obs.observers
WHERE observer_name = $1;

-- name: CreateObservationsAsPercentUsingBatch :batchexec
-- CreateObservationsAsPercentUsingBatch inserts observed yields as a batch process.
-- Input yields are expected as a percentage of capacity. This is more readable but
-- slower than using the COPY protocol.
INSERT INTO obs.observed_generation_values (
    location_id, source_type_id, observer_id, observation_time_utc, value
) VALUES (
    $1, (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2), $3, $4,
    encode_pct(sqlc.arg(yield_pct)::real)
);

-- name: CreateObservationsAsInt16UsingCopy :copyfrom
-- CreateObservationsCopy inserts a batch of observations using postgres COPY protocol,
-- making it the fastest way to perform large inserts of observations.
-- Input yields are expected as 16-bit integers, with 0 representing 0%
-- and 30000 representing 100% of capacity.
INSERT INTO obs.observed_generation_values (
    location_id, source_type_id, observer_id, observation_time_utc, value
) VALUES (
    $1, $2, $3, $4, $5
);

-- name: GetObservationsAsPercentBetween :many
-- GetObservationsAsPercent gets observations between two timestamps
-- and returns their values as percentage of capacity.
-- Has been measured to be 10 times slower than returning the values directly, so use in non-critical paths
-- where readability or understandability is more important than performance.
SELECT
    location_id,
    source_type_id,
    observation_time_utc,
    decode_smallint(value)::real AS yield_pct
FROM obs.observed_generation_values
WHERE
    location_id = $1
    AND source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    AND observer_id = (SELECT observer_id FROM obs.observers WHERE observer_name = $3)
    AND observation_time_utc BETWEEN sqlc.arg(start_time_utc)::timestamp AND sqlc.arg(end_time_utc)::timestamp;

-- name: GetObservationsAsInt16Between :many
-- GetObservationsAsInt16 gets observations between two timestamps
-- and returns their values as 16-bit integers, with 0 representing 0%
-- and 30000 representing 100% of capacity. This is faster than converting the values to percentages.
SELECT
    location_id,
    source_type_id,
    observation_time_utc,
    value
FROM obs.observed_generation_values
WHERE
    location_id = $1
    AND source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    AND observer_id = (SELECT observer_id FROM obs.observers WHERE observer_name = $3)
    AND observation_time_utc BETWEEN sqlc.arg(start_time_utc)::timestamp AND sqlc.arg(end_time_utc)::timestamp;


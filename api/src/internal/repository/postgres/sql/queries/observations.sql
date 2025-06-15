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

-- name: CreateObservation :exec
INSERT INTO obs.observed_generation_values (
    location_id, source_type_id, observer_id, observation_time_utc, value
) VALUES (
    $1, (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2), $3, $4,
    encode_pct(sqlc.arg(yield_pct)::real)
);

-- name: BatchCreateObservations :batchexec
INSERT INTO obs.observed_generation_values (
    location_id, source_type_id, observer_id, observation_time_utc, value
) VALUES (
    $1, (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2), $3, $4,
    encode_pct(sqlc.arg(yield_pct)::real)
);

-- name: CopyCreateObservations :copyfrom
INSERT INTO obs.observed_generation_values (
    location_id, source_type_id, observer_id, observation_time_utc, value
) VALUES (
    $1, $2, $3, $4, $5
);

-- name: GetObservations :many
SELECT
    location_id, source_type_id, observation_time_utc,
    decode_smallint(value)::real AS yield_pct
FROM obs.observed_generation_values
WHERE
    location_id = $1
    AND source_type_id = (SELECT source_type_id FROM loc.source_types WHERE source_type_name = $2)
    AND observer_id = (SELECT observer_id FROM obs.observers WHERE observer_name = $3)
    AND observation_time_utc = ANY(sqlc.arg(observation_time_utc)::timestamp[]);

-- name: GetObservationsBetween :many
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

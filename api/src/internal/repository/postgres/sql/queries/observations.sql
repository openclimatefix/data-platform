-- name: CreateObservation :one
WITH source_type_id AS (
    SELECT source_type_id FROM loc.source_types
    WHERE source_type_name = $2
)
INSERT INTO obs.observed_generation_values (
    location_id, source_type_id, time_utc, value
) VALUES (
    $1, source_type_id, $3, $4
) RETURNING observation_id;

-- name: CreateObservations :copyfrom
INSERT INTO obs.observed_generation_values (
    location_id, source_type_id, time_utc, value
) VALUES (
    $1, $2, $3, $4
);

-- name: ListObservationsByLocationId :many
SELECT * FROM obs.observed_generation_values
WHERE location_id = $1;

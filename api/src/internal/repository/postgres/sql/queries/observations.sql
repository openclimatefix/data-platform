-- name: CreateObservation :one
INSERT INTO obs.observations (
    location_id, time_utc, generation, generation_unit_prefix_factor
) VALUES (
    $1, $2, $3, $4
) RETURNING observation_id;

-- name: CreateObservations :copyfrom
INSERT INTO obs.observations (
    location_id, time_utc, generation, generation_unit_prefix_factor
) VALUES (
    $1, $2, $3, $4
);

-- name: ListObservations :many
SELECT
    obs.observations.observation_id,
    obs.observations.location_id,
    obs.observations.time_utc,
    obs.observations.generation,
    obs.observations.generation_unit_prefix_factor
FROM obs.observations;

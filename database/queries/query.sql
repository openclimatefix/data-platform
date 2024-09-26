--name: GetObservations :one
SELECT * FROM obs.observations WHERE id = $1 LIMIT 1;

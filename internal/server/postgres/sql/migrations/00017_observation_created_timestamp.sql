-- +goose Up

ALTER TABLE obs.observed_generation_values ADD COLUMN created_timestamp_utc TIMESTAMP;

-- Existing observations are backfilled with the time of the observation itself.
UPDATE obs.observed_generation_values SET created_timestamp_utc = observation_timestamp_utc;

ALTER TABLE obs.observed_generation_values
ALTER COLUMN created_timestamp_utc SET DEFAULT CURRENT_TIMESTAMP,
ALTER COLUMN created_timestamp_utc SET NOT NULL;

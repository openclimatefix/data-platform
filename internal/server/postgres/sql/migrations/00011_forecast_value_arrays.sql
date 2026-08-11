-- +goose Up
DROP FUNCTION IF EXISTS uuidv7_extract_timestamp(UUID);

-- +goose StatementBegin
CREATE FUNCTION uuidv7_extract_timestamp(u UUID) RETURNS TIMESTAMP
   LANGUAGE sql
   IMMUTABLE STRICT PARALLEL SAFE
   RETURN uuid_extract_timestamp(u) AT TIME ZONE 'UTC';
-- +goose StatementEnd

/*
 * Moves predicted values from separate table into arrays.
 *
 * Array index i (1-based) corresponds to target time:
 *     target_time = LOWER(target_period) + (i - 1) * value_resolution_mins
 * Only works if a forecast has evenly spaced target times.
 */

ALTER TABLE pred.forecasts
    ADD COLUMN p02_sips SMALLINT [],
    ADD COLUMN p10_sips SMALLINT [],
    ADD COLUMN p25_sips SMALLINT [],
    ADD COLUMN p50_sips SMALLINT [],
    ADD COLUMN p75_sips SMALLINT [],
    ADD COLUMN p90_sips SMALLINT [],
    ADD COLUMN p98_sips SMALLINT [];

ALTER TABLE pred.forecasts
    ADD CONSTRAINT plevel_lengths_match_check CHECK (
        p50_sips IS NULL OR (
            ARRAY_LENGTH(p50_sips, 1) > 0
            AND COALESCE(ARRAY_LENGTH(p02_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
            AND COALESCE(ARRAY_LENGTH(p10_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
            AND COALESCE(ARRAY_LENGTH(p25_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
            AND COALESCE(ARRAY_LENGTH(p75_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
            AND COALESCE(ARRAY_LENGTH(p90_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
            AND COALESCE(ARRAY_LENGTH(p98_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        )
    ) NOT VALID;

/*
 * I want init time to be VIRTUAL, but sqlc doesn't support it yet.
 * See https://github.com/sqlc-dev/sqlc/issues/4322. Until then it stays a plain NOT NULL
 * column written by the application, so a rebuild that forgets to carry it over fails loudly
 * rather than silently nulling the column every read query derives horizons from.
 *
 * The recency check is dropped: it referenced CURRENT_TIMESTAMP, which is not
 * immutable, so it could not be revalidated and breaks ATTACH PARTITION.
 */
ALTER TABLE pred.forecasts
    DROP CONSTRAINT IF EXISTS init_time_utc_recency_check;

-- +goose Down
ALTER TABLE pred.forecasts
    ADD CONSTRAINT init_time_utc_recency_check CHECK (
        init_time_utc >= '2000-01-01 00:00:00'::TIMESTAMP
        AND init_time_utc < CURRENT_TIMESTAMP + MAKE_INTERVAL(days => 30)
    ) NOT VALID;

ALTER TABLE pred.forecasts
    DROP CONSTRAINT IF EXISTS plevel_lengths_match_check,
    DROP COLUMN p02_sips, DROP COLUMN p10_sips, DROP COLUMN p25_sips,
    DROP COLUMN p50_sips, DROP COLUMN p75_sips, DROP COLUMN p90_sips,
    DROP COLUMN p98_sips;

DROP FUNCTION IF EXISTS uuidv7_extract_timestamp(UUID);

-- +goose StatementBegin
CREATE FUNCTION uuidv7_extract_timestamp(UUID) RETURNS TIMESTAMPTZ
AS $$
 SELECT to_timestamp(
   right(substring(uuid_send($1) from 1 for 6)::text, -1)::bit(48)::int8
    /1000.0);
$$ LANGUAGE sql immutable strict parallel safe;
-- +goose StatementEnd

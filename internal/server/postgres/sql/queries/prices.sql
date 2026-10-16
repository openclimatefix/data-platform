-- name: CreateMarket :one
INSERT INTO obs.markets (market_name, currency_code, country_code) VALUES (
    LOWER(sqlc.arg(market_name)::TEXT), UPPER(sqlc.arg(currency_code)::TEXT), UPPER(sqlc.arg(country_code)::TEXT)
) RETURNING
    market_uuid, market_name, currency_code, country_code;

-- name: GetMarketsByFilters :many
SELECT
    market_uuid,
    market_name,
    currency_code,
    country_code
FROM obs.markets
WHERE (
    ARRAY_LENGTH(sqlc.arg(market_names)::TEXT[], 1) IS NULL
    OR market_name = ANY(ARRAY(
        SELECT LOWER(n) FROM UNNEST(sqlc.arg(market_names)::TEXT[]) AS n
    ))
);

-- name: GetMarketByName :one
SELECT
    market_uuid,
    market_name,
    currency_code,
    country_code
FROM obs.markets
WHERE market_name = LOWER(sqlc.arg(market_name)::TEXT);

-- name: CreatePriceValues :exec
/* CreatePriceValues upserts a set of prices that share a location, market, resolution
 * and created time. Prices are given as integers in hundredths of the market currency per MWh.
 * Republished prices replace those already stored; those that are unchanged are left alone so
 * that the created timestamp keeps reflecting when the price last changed.
 */
INSERT INTO obs.observed_price_values AS p (
    market_uuid,
    period_start_utc,
    resolution_mins,
    price_hundredths_per_mwh,
    created_timestamp_utc
)
SELECT
    sqlc.arg(market_uuid)::UUID,
    t.period_start_utc,
    sqlc.arg(resolution_mins)::SMALLINT,
    pr.price_hundredths_per_mwh,
    sqlc.arg(created_timestamp_utc)::TIMESTAMP
FROM UNNEST(sqlc.arg(period_starts_utc)::TIMESTAMP[]) WITH ORDINALITY AS t (period_start_utc, ord)
    INNER JOIN
        UNNEST(sqlc.arg(prices_hundredths_per_mwh)::INTEGER[]) WITH ORDINALITY AS pr (price_hundredths_per_mwh, ord)
    ON t.ord = pr.ord
ON CONFLICT (market_uuid, period_start_utc)
DO UPDATE SET
    resolution_mins = excluded.resolution_mins,
    price_hundredths_per_mwh = excluded.price_hundredths_per_mwh,
    created_timestamp_utc = excluded.created_timestamp_utc
WHERE (p.resolution_mins, p.price_hundredths_per_mwh)
IS DISTINCT FROM (excluded.resolution_mins, excluded.price_hundredths_per_mwh);

-- name: GetPricesBetween :many
/* GetPricesBetween gets prices whose period starts between two timestamps (inclusive),
 * as integers in hundredths of the market currency per MWh.
 */
SELECT
    period_start_utc,
    resolution_mins,
    price_hundredths_per_mwh,
    created_timestamp_utc
FROM obs.observed_price_values
WHERE
    market_uuid = $1
    AND period_start_utc BETWEEN sqlc.arg(start_time_utc)::TIMESTAMP AND sqlc.arg(end_time_utc)::TIMESTAMP
ORDER BY period_start_utc ASC;

-- +goose Up

/* == ENERGY PRICES ===============================================================================
 *
 * Tables to handle observed energy market prices.
 *
 * Prices are used, for example, to infer curtailment: when the price is negative for some time
 * and then returns to positive, generation is likely to have been curtailed in between.
 *
 * Unlike generation values, prices are not a fraction of a source's capacity, and are known ahead
 * of time (e.g. day-ahead prices are published the day before delivery). They are therefore
 * stored in their own table, keyed by the market that produced them.
 */

/*
 * Table to store markets.
 * A market is a producer of energy prices, e.g. a day-ahead or intraday auction. The currency is
 * a property of the market, so prices of differing currencies are never mixed in one series.
 */
CREATE TABLE obs.markets (
    market_uuid UUID NOT NULL DEFAULT UUIDV7(),
    market_name TEXT NOT NULL,
    CONSTRAINT market_name_format_check CHECK (
        LENGTH(market_name) > 0 AND LENGTH(market_name) < 128
        AND market_name = LOWER(market_name)
    ),
    currency_code CHAR(3) NOT NULL,
    CONSTRAINT currency_code_format_check CHECK (currency_code ~ '^[A-Z]{3}$'),
    country_code CHAR(2) NOT NULL,
    CONSTRAINT country_code_format_check CHECK (country_code ~ '^[A-Z]{2}$'),
    PRIMARY KEY (market_uuid),
    UNIQUE (market_name)
);

/*
 * Table to store observed energy prices.
 * The price is stored as an integer number of hundredths of the market's currency unit per MWh
 * (e.g. 4567 is 45.67 EUR/MWh), and may be negative. The period covered by a price starts at
 * the period start and lasts for the resolution; the resolution is stored since markets change
 * it over time (e.g. hourly to 15-minute day-ahead periods).
 * Republished prices replace the existing ones, in which case the created timestamp is updated.
 */
CREATE TABLE obs.observed_price_values (
    market_uuid UUID NOT NULL
    REFERENCES obs.markets (market_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    period_start_utc TIMESTAMP NOT NULL,
    CONSTRAINT period_start_utc_recency_check CHECK (
        period_start_utc <= CURRENT_TIMESTAMP + MAKE_INTERVAL(days => 31)
    ),
    resolution_mins SMALLINT NOT NULL,
    CONSTRAINT resolution_mins_size_check CHECK (
        resolution_mins > 0 AND resolution_mins <= 60
    ),
    price_hundredths_per_mwh INTEGER NOT NULL,
    created_timestamp_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (market_uuid, period_start_utc)
);

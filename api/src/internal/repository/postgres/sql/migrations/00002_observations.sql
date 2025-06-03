-- +goose Up

/*
Schema and tables to handle observed generation data.

Observations of generation data is usually measured by providers of inverters,
which are required in many sources of renewable energy to convert power from DC to AC.
Partnerships with these providers provide access to the data in order to
test the accuracy of predictions.
*/

CREATE SCHEMA obs;

/*- Tables ----------------------------------------------------------------------------------*/

/* 
Table to store observers.
These are providers of actual recorded generation values from inverters
(mostly - looking at you, pvlive...)
*/
CREATE TABLE obs.observers (
    observer_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    observer_name TEXT NOT NULL
        CHECK (
            LENGTH(observer_name) > 0 AND LENGTH(observer_name) < 128
            AND observer_name = LOWER(observer_name)
        ),
    PRIMARY KEY (observer_id),
    UNIQUE (observer_name)
);

-- Table to store observed generation values
CREATE TABLE obs.observed_generation_values (
    -- The generation value as a percentage of the location capacity
    -- represented by the smallint range. Since it isn't impossible to measure
    -- a little over capacity, 30000 represents 100% of capacity instead of the
    -- max smallint value (32767). This allows for some measurement leeway.
    value SMALLINT NOT NULL
        CHECK ( value >= 0 ),
    source_type_id SMALLINT NOT NULL
        REFERENCES loc.source_types(source_type_id)
        ON DELETE RESTRICT,
    observer_id INTEGER NOT NULL
        REFERENCES obs.observers(observer_id)
        ON DELETE CASCADE,
    location_id INTEGER NOT NULL
        REFERENCES loc.locations(location_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    observation_time_utc TIMESTAMP NOT NULL
        CHECK ( observation_time_utc <= CURRENT_TIMESTAMP ),
    PRIMARY KEY (location_id, source_type_id, observer_id, observation_time_utc)
);

/*- Functions -----------------------------------------------------------------*/

CREATE OR REPLACE FUNCTION obs.encode_percent_as_smallint(value REAL) RETURNS SMALLINT
    LANGUAGE plpgsql
    IMMUTABLE
AS $$
BEGIN
    -- Convert a float value in the range [0.0, 1.0] to a smallint in the range [0, 30000]
    -- Allow a little leeway for measurement errors by using 30000 as the max value,
    -- so values up to 1.09375 (32767 / 30000) are allowed.
    IF value < 0.0 OR value > (32767::smallint * 100 / 30000)::real THEN
        RAISE EXCEPTION 'Percentage value % must be in the range [0.0, 109.3]', value;
    END IF;
    RETURN CAST(value * 30000 AS SMALLINT);
END;

CREATE OR REPLACE FUNCTION obs.decode_smallint_to_percent(value SMALLINT) RETURNS REAL
    LANGUAGE plpgsql
    IMMUTABLE
AS $$
BEGIN
    RETURN CAST(value AS REAL) * 100 / 30000.0;
END;

-- +goose Down
DROP SCHEMA obs CASCADE;

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

-- Table to store observed generation values
CREATE TABLE obs.observed_generation_values (
    -- The generation value as a percentage of the location capacity
    value SMALLINT NOT NULL
        CHECK ( value >= 0 AND value <= 110 ),
    source_type_id SMALLINT NOT NULL
        REFERENCES loc.source_types(source_type_id)
        ON DELETE RESTRICT,
    observation_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    location_id INTEGER NOT NULL
        REFERENCES loc.locations(location_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    time_utc TIMESTAMP NOT NULL
        CHECK ( time_utc <= CURRENT_TIMESTAMP ),
    -- Observed generation in factors of Watts
    PRIMARY KEY (observation_id),
    UNIQUE (location_id, time_utc)
);

-- +goose Down
DROP SCHEMA obs CASCADE;

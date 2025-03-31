/*
Schema and tables to handle observed generation data.

Observations of generation data is usually measured by providers of inverters,
which are required in many sources of renewable energy to convert power from DC to AC.
Partnerships with these providers provide access to the data in order to
test the accuracy of predictions.
*/

-- +goose Up
-- +goose StatementBegin

CREATE SCHEMA obs;
COMMENT ON SCHEMA obs IS 'Data for observed generation';

/*- Tables ----------------------------------------------------------------------------------*/

CREATE TABLE obs.observations (
    observation_id INT GENERATED ALWAYS AS IDENTITY NOT NULL,
    location_id INT NOT NULL
        REFERENCES loc.locations(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    time_utc TIMESTAMP NOT NULL
        CHECK ( time_utc <= CURRENT_TIMESTAMP ),
    generation SMALLINT NOT NULL
        CHECK ( generation >= 0 ),
    generation_unit_prefix_factor SMALLINT DEFAULT (0) NOT NULL
        CHECK ( unit_prefix_factor IN (0, 3, 6, 9, 12) ),
    PRIMARY KEY (id),
    UNIQUE (location_id, time_utc)
);
COMMENT ON TABLE obs.observations IS 'Observed generation data, in factors of Watts.';
COMMENT ON COLUMN obs.observations.observation_id IS 'Unique identifier for the observation.';
COMMENT ON COLUMN obs.observations.location_id IS 'Location of the observed generation.';
COMMENT ON COLUMN obs.observations.time_utc IS 'Time of the observation in UTC.';
COMMENT ON COLUMN obs.observations.generation IS 'Numeric value associated with generation. Multiply by 10 to the power of unit_prefix_factor to get the actual value.';
COMMENT ON COLUMN obs.observations.generation_unit_prefix_factor IS 'Factor defining the metric prefix of the generation value. Raise 10 to the power of this value to get the prefix.';

-- +goose StatementEnd


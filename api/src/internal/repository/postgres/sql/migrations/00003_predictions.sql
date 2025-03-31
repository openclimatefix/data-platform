/*
Schema and tables to handle predicted generation data.

Predicted generation data is produced by various forecast models specific to a location.
A forecast is a set of predicted generations, beginning at the
*initialisation time*. Each subsequent generation's *target time* is equivalent to the
initialisation time plus the *horizon*.

From a frontend standpoint, the latest produced forecast is the most accurate
for a given location.

 */

-- +goose Up
-- +goose StatementBegin

CREATE SCHEMA pred;
COMMENT ON SCHEMA pred IS 'Data for predicted generation';

/*- Functions -------------------------------------------------------------------------------*/

CREATE OR REPLACE FUNCTION update_crosssection() RETURNS TRIGGER AS $update_crossection$
BEGIN
    INSERT INTO pred.predicted_generation_crossections(
        forecast_id,
        location_id,
        target_time_utc,
        generation,
        generation_unit_prefix_factor,
        horizon_mins,
        model_id
    ) SELECT (
        NEW.forecast_id,
        f.location_id,
        f.init_time_utc + new.horizon_mins * INTERVAL '1 minute' AS target_time_utc,
        NEW.generation,
        NEW.generation_unit_prefix_factor,
        NEW.horizon_mins,
        f.model_id
    ) FROM NEW JOIN pred.forecasts as f USING (forecast_id)
    ON CONFLICT (location_id, target_time_utc, model_id)
    DO UPDATE SET 
        generation = EXCLUDED.generation,
        generation_unit_prefix_factor = EXCLUDED.generation_unit_prefix_factor;

    -- Clean up old cross-section data
    DELETE FROM pred.predicted_generation_crossections
    WHERE target_time_utc < CURRENT_TIMESTAMP - INTERVAL '5 days';

    RETURN NULL; -- This is an After trigger so don't return anything
END;
$update_crossection$ language plpgsql;

/*- Tables ----------------------------------------------------------------------------------*/

/*
A forecast model is an ML model that generated predicted generation values.
Each model's name and version number uniquely identifies it.
*/
CREATE TABLE pred.models (
    model_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    name TEXT NOT NULL
        CHECK ( LENGTH(name) > 0 and LENGTH(name) < 64 ),
    version TEXT NOT NULL
        CHECK ( LENGTH(version) > 0 and LENGTH(version) < 64 ),
    created_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (model_id),
    UNIQUE (name, version)
);
COMMENT ON TABLE pred.models IS 'Model used to generate a forecast';
COMMENT ON COLUMN pred.models.model_id IS 'Unique identifier for a forecast model';
COMMENT ON COLUMN pred.models.name IS 'Name of the forecast model';
COMMENT ON COLUMN pred.models.version IS 'Version of the forecast model';
COMMENT ON COLUMN pred.models.created_at_utc IS 'Time the model was created';

/*
Forecasts refer to the generation predictions created by a specific version
of a forecast model for a specific location with a specific initialization time.
There can only be one forecast per location per initialization time per model,
reruns should replace old values.
*/
CREATE TABLE pred.forecasts (
    forecast_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    location_id INT NOT NULL
        REFERENCES loc.locations(location_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    init_time_utc TIMESTAMP NOT NULL,
    model_id INT NOT NULL
        REFERENCES pred.models(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    PRIMARY KEY (forecast_id),
    UNIQUE (location_id, init_time_utc, model_id) -- TODO: Think about ordering
);
COMMENT ON TABLE pred.forecasts IS 'Metadata for a forecast';
COMMENT ON COLUMN pred.forecasts.forecast_id IS 'Unique identifier for a forecast';
COMMENT ON COLUMN pred.forecasts.location_id IS 'Location the forecast is for';
COMMENT ON COLUMN pred.forecasts.init_time_utc IS 'Initialization time of the forecast';
COMMENT ON COLUMN pred.forecasts.model_id IS 'Model used to generate the forecast';

/*
Predicted generation values are the output of a forecast model.
There can only be one predicted generation value per forecast per horizon.
This table gets very large very quickly, so to save space, data is stored as smallints
where possible. However, because generation can be for locations that vary greatly in
size, we also store the unit of the generation.

- BIGINT: 8 bytes for 0W-2.14MW
- SMALLINT + SMALLINT: 2+2 = 4 bytes for 0W-32000TW+
*/
CREATE TABLE pred.predicted_generation_values (
    forecast_id INT NOT NULL
        REFERENCES pred.forecasts(forecast_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    target_time_utc TIMESTAMP NOT NULL,
    horizon_mins SMALLINT NOT NULL,
        CHECK (horizon_mins >= 0),
    generation SMALLINT NOT NULL,
        CHECK (generation >= 0),
    generation_unit_prefix_factor SMALLINT DEFAULT (0) NOT NULL
        CHECK ( unit_prefix_factor IN (0, 3, 6, 9, 12) ),
    PRIMARY KEY (horizon_mins, forecast_id) -- Horizon first to ensure it can be queried standalone
);
COMMENT ON TABLE pred.predicted_generation_values IS 'Predicted generation data, in factors of Watts';
COMMENT ON COLUMN pred.predicted_generation_values.forecast_id IS 'Unique identifier for a forecast';
COMMENT ON COLUMN pred.predicted_generation_values.horizon_mins IS 'Time horizon in mins for generation value';
COMMENT ON COLUMN pred.predicted_generation_values.generation IS 'Numeric value associated with predicted generation. Multiply by 10 raised to the power of unit_prefix_factor to get the actual value in Watts';
COMMENT ON COLUMN pred.predicted_generation_values.generation_unit_prefix_factor IS 'Factor defining the metric prefix of the generation value. Raise 10 to the power of this value to get the metric prefix.';

CREATE TRIGGER update_crosssection
    AFTER INSERT OR UPDATE ON pred.predicted_generation_values
    FOR EACH ROW EXECUTE FUNCTION update_crosssection();

/*
Cross-sections contain the all predicted generation values for a specific location that
have the smallest horizon; i.e. were predicted the closest to their target time.
*/
CREATE TABLE pred.predicted_generation_crosssections (
    forecast_id INT NOT NULL
        REFERENCES pred.forecasts(forecast_id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    location_id INT NOT NULL
        REFERENCES loc.locations(location_id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE,
    target_time_utc TIMESTAMP NOT NULL,
    horizon_mins SMALLINT NOT NULL,
        CHECK (horizon_mins >= 0),
    generation SMALLINT NOT NULL,
        CHECK (generation >= 0),
    generation_unit_prefix_factor SMALLINT DEFAULT (0) NOT NULL
        CHECK ( unit_prefix_factor IN (0, 3, 6, 9, 12) ),
    model_id INT NOT NULL
        REFERENCES pred.models(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    PRIMARY KEY (location_id, target_time_utc, model_id)
);
COMMENT ON TABLE pred.predicted_generation_crosssections IS 'Cross-section of predicted generation data for locations';
COMMENT ON COLUMN pred.predicted_generation_crosssections.forecast_id IS 'Unique identifier for a forecast';
COMMENT ON COLUMN pred.predicted_generation_crosssections.location_id IS 'Location the forecast is for';
COMMENT ON COLUMN pred.predicted_generation_crosssections.target_time_utc IS 'Time the generation was predicted for';
COMMENT ON COLUMN pred.predicted_generation_crosssections.horizon_mins IS 'Time horizon in mins for generation value';
COMMENT ON COLUMN pred.predicted_generation_crosssections.generation IS 'Numeric value associated with predicted generation. Multiply by 10 raised to the power of unit_prefix_factor to get the actual value in Watts';
COMMENT ON COLUMN pred.predicted_generation_crosssections.generation_unit_prefix_factor IS 'Factor defining the metric prefix of the generation value. Raise 10 to the power of this value to get the metric prefix.';
COMMENT ON COLUMN pred.predicted_generation_crosssections.model_id IS 'Model used to generate the forecast';


-- +goose StatementEnd


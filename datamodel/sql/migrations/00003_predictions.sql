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

/*- Tables ----------------------------------------------------------------------------------*/

/*
A forecast model is an ML model that generated predicted generation values.
Each forecast model's name and version number uniquely identifies it.
*/
CREATE TABLE pred.forecast_models (
    id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    name TEXT NOT NULL
        CHECK ( LENGTH(name) > 0 and LENGTH(name) < 126 ),
    version TEXT NOT NULL
        CHECK ( LENGTH(version) > 0 and LENGTH(version) < 64 ),
    PRIMARY KEY (id),
    UNIQUE (name, version)
);
COMMENT ON TABLE pred.forecast_models IS 'Model used to generate a forecast';
COMMENT ON COLUMN pred.forecast_models.id IS 'Unique identifier for a forecast model';
COMMENT ON COLUMN pred.forecast_models.name IS 'Name of the forecast model';
COMMENT ON COLUMN pred.forecast_models.version IS 'Version of the forecast model';

/*
Forecasts refer to the generation predictions created by a specific version
of a forecast model for a specific location with a specific initialization time.
There can only be one forecast per location per initialization time per model,
reruns should replace old values.
*/
CREATE TABLE pred.forecasts (
    id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    location_id INT NOT NULL
        REFERENCES loc.locations(id),
    created_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    init_time_utc TIMESTAMP NOT NULL,
    forecast_model_id INT NOT NULL
        REFERENCES pred.forecast_models(id),
    PRIMARY KEY (id),
    UNIQUE (location_id, init_time_utc, forecast_model_id)
);
COMMENT ON TABLE pred.forecasts IS 'Metadata for a forecast';
COMMENT ON COLUMN pred.forecasts.id IS 'Unique identifier for a forecast';
COMMENT ON COLUMN pred.forecasts.location_id IS 'Location the forecast is for';
COMMENT ON COLUMN pred.forecasts.created_utc IS 'Time the forecast was created';
COMMENT ON COLUMN pred.forecasts.init_time_utc IS 'Initialization time of the forecast';
COMMENT ON COLUMN pred.forecasts.forecast_model_id IS 'Model used to generate the forecast';


/*
Predicted generation values are the output of a forecast model.
There can only be one predicted generation value per forecast per horizon.
This table gets very large very quickly, so to save space, data is stored as smallints
where possible. However, because generation can be for locations that vary greatly in
size, we also store the unit of the generation.

- BIGINT: 8 bytes for 0W-2.14MW
- SMALLINT + SMALLINT: 2+2 = 4 bytes for 0W-32000TW+
*/
CREATE TABLE pred.predicted_generation (
    forecast_id INT NOT NULL
        REFERENCES pred.forecasts(id),
    horizon_mins SMALLINT NOT NULL,
    CHECK (horizon_mins >= 0),
    generation SMALLINT NOT NULL,
    CHECK (generation >= 0),
    generation_units SMALLINT NOT NULL
        REFERENCES obs.power_units(id),
    PRIMARY KEY (forecast_id, horizon_mins)
);
COMMENT ON TABLE pred.predicted_generation IS 'Predicted generation values';
COMMENT ON COLUMN pred.predicted_generation.forecast_id IS 'Unique identifier for a forecast';
COMMENT ON COLUMN pred.predicted_generation.horizon_mins IS 'Time horizon in mins for generation value';
COMMENT ON COLUMN pred.predicted_generation.generation IS 'Predicted generation value';
COMMENT ON COLUMN pred.predicted_generation.generation_units IS 'Units of the generation value';

-- +goose StatementEnd


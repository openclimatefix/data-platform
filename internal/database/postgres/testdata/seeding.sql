
-- Function to seed values into the database
CREATE OR REPLACE FUNCTION seed_db(
    num_locations INTEGER DEFAULT 1000,
    gv_resolution_mins INTEGER DEFAULT 30,
    forecast_resolution_mins INTEGER DEFAULT 30,
    forecast_length_mins INTEGER DEFAULT 480,
    num_forecasts_per_location INTEGER DEFAULT 24,
    num_models INTEGER DEFAULT 1,
    pivot_time TIMESTAMP DEFAULT DATE_TRUNC('hour', NOW())
)
RETURNS INTEGER AS $$
DECLARE
    loc_id INTEGER;
    src_id INTEGER;
    p_id INTEGER;
    result RECORD;
    num_pgvs_per_forecast INTEGER := forecast_length_mins / gv_resolution_mins;
    earliest_forecast_offset_mins INTEGER := num_forecasts_per_location * forecast_resolution_mins;
BEGIN
    -- Insert predictors
    INSERT INTO pred.predictors (predictor_name, predictor_version)
    SELECT
        'test_model_' || i,
        'v1'
    FROM generate_series(1, num_models) AS i;

    -- Insert locations
    INSERT INTO loc.locations
      (location_name, location_type_id, geom)
    SELECT
        'TESTLOCATION' || i AS location_name,
        (SELECT location_type_id FROM loc.location_types WHERE location_type_name = 'SITE'),
        ST_SetSRID(ST_MakePoint(random() * 360 - 180, random() * 180 - 90), 4326)
    FROM generate_series(0, num_locations - 1) as i;
    RAISE NOTICE 'Inserted % locations', (SELECT COUNT(*) FROM loc.locations);

    -- Insert observers
    INSERT INTO obs.observers (observer_name) VALUES ('test_observer');

    FOR loc_id IN SELECT location_id FROM loc.locations LOOP

        INSERT INTO loc.sources
            (source_type_id, capacity, capacity_unit_prefix_factor, location_id, source_version, metadata)
        VALUES (
            (SELECT source_type_id FROM loc.source_types WHERE source_type_name = 'SOLAR'),
            1000::SMALLINT,
            3,
            loc_id,
            5,
            jsonb_build_object('source', 'test'))
        RETURNING source_id INTO src_id;

        INSERT INTO loc.sources_history
            (source_id, capacity, capacity_unit_prefix_factor, source_version, sys_period)
        SELECT
            src_id,
            200 * i::SMALLINT,
            3,
            i,
            CASE
                WHEN i = 1 THEN
                    TSRANGE(NULL, pivot_time - '4 years'::interval, '[)')
                WHEN i = 5 THEN
                    TSRANGE(pivot_time, NULL, '[)')
                ELSE
                    TSRANGE(pivot_time + make_interval(years=>i-5), pivot_time + make_interval(years=>i-4), '[)')
            END
        FROM generate_series(1, 5) AS i
        ON CONFLICT (source_id, source_version) DO NOTHING;

        -- Insert forecasts for each location and model
        FOR p_id IN SELECT predictor_id FROM pred.predictors LOOP
            INSERT INTO pred.forecasts
                (source_type_id, location_id, predictor_id, init_time_utc)
            SELECT
                (SELECT source_type_id FROM loc.source_types WHERE source_type_name = 'SOLAR'),
                loc_id,
                p_id,
                pivot_time - (i || ' minutes')::interval
            FROM generate_series(0, earliest_forecast_offset_mins - forecast_resolution_mins, forecast_resolution_mins) AS i;
        END LOOP; 

        -- Insert observed generation values covering all the forecast period, always half the capacity
        INSERT INTO obs.observed_generation_values
            (value_sip, source_type_id, observer_id, location_id, observation_time_utc)
        SELECT
            15000::SMALLINT,
            (SELECT source_type_id FROM loc.source_types WHERE source_type_name = 'SOLAR'),
            (SELECT observer_id FROM obs.observers WHERE observer_name = 'test_observer'),
            loc_id,
            pivot_time - (i || ' minutes')::interval
        FROM generate_series(0, earliest_forecast_offset_mins - gv_resolution_mins, gv_resolution_mins) AS i;

    END LOOP;
    RAISE NOTICE 'Inserted % observed generation values', (SELECT COUNT(*) FROM obs.observed_generation_values);

    -- Insert predicted generation values for each forecast
    FOR result IN SELECT forecast_id, init_time_utc FROM pred.forecasts LOOP
        INSERT INTO pred.predicted_generation_values
            (horizon_mins, p10_sip, p50_sip, p90_sip, forecast_id, target_time_utc, metadata)
        SELECT
            i,
            GREATEST(CAST((100 / num_pgvs_per_forecast) * (i / gv_resolution_mins) * (30000/100) AS SMALLINT) - 300::SMALLINT, 0::SMALLINT),
            CAST((100 / num_pgvs_per_forecast) * (i / gv_resolution_mins) * (30000/100) AS SMALLINT),
            CAST((100 / num_pgvs_per_forecast) * (i / gv_resolution_mins) * (30000/100) AS SMALLINT) + 300::SMALLINT,
            result.forecast_id,
            result.init_time_utc + (i || ' minutes')::interval,
            jsonb_build_object('source', 'test')
        FROM generate_series(0, forecast_length_mins - gv_resolution_mins, gv_resolution_mins) AS i;
    END LOOP;
    RAISE NOTICE 'Inserted % predicted generation values', (SELECT COUNT(*) FROM pred.predicted_generation_values);

    RETURN(SELECT COUNT(*) from pred.predicted_generation_values);
END;
$$ LANGUAGE plpgsql;


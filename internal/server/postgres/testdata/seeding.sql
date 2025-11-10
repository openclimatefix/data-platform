-- Function to seed values into the database
CREATE OR REPLACE FUNCTION seed_db(
    name_prefix TEXT,
    num_locations INTEGER DEFAULT 1000,
    gv_resolution_mins INTEGER DEFAULT 30,
    forecast_resolution_mins INTEGER DEFAULT 30,
    forecast_length_mins INTEGER DEFAULT 480,
    num_forecasts_per_location INTEGER DEFAULT 24,
    pivot_time TIMESTAMP DEFAULT DATE_TRUNC('hour', NOW())
)
RETURNS TABLE (num_values INTEGER, geometry_uuids UUID[]) AS $$
DECLARE
    geo_id UUID;
    p_id INTEGER;
    result RECORD;
    num_pgvs_per_forecast INTEGER := forecast_length_mins / gv_resolution_mins;
    earliest_forecast_offset_mins INTEGER := num_forecasts_per_location * forecast_resolution_mins;
BEGIN
    -- Insert forecasters
    INSERT INTO pred.forecasters (forecaster_name, forecaster_version)
    VALUES (LOWER(name_prefix) || '_forecaster', 'v1');

    -- Insert geometries
    INSERT INTO loc.geometries
      (geometry_name, geometry_type_id, geom)
    SELECT
        LOWER(name_prefix) || '_testgeometry' || i AS geometry_name,
        1,
        ST_SetSRID(ST_MakePoint(random() * 355 - 180, random() * 175 - 90), 4326)
    FROM generate_series(0, num_locations - 1) as i;
    RAISE NOTICE 'Inserted % geometries', (SELECT COUNT(*) FROM loc.geometries);

    -- Insert observers
    INSERT INTO obs.observers (observer_name) VALUES (LOWER(name_prefix) || '_observer');

    FOR geo_id IN SELECT geometry_uuid FROM loc.geometries LOOP

        INSERT INTO loc.sources_history
            (geometry_uuid, source_type_id, capacity, capacity_unit_prefix_factor, valid_from_utc)
        SELECT
            geo_id,
            1,
            200 * i::SMALLINT,
            3,
            pivot_time + make_interval(years=>i-5)
        FROM generate_series(1, 5) AS i;

        -- Insert forecasts for each location and model
        FOR p_id IN SELECT forecaster_id FROM pred.forecasters LOOP
            INSERT INTO pred.forecasts
                (source_type_id, geometry_uuid, forecaster_id, init_time_utc, value_resolution_mins, target_period)
            SELECT
                1,
                geo_id,
                p_id,
                pivot_time - (i || ' minutes')::interval,
                gv_resolution_mins::SMALLINT,
                TSRANGE(
                    pivot_time - (i || ' minutes')::interval,
                    pivot_time - (i || ' minutes')::interval + make_interval(mins => forecast_length_mins)
                )
            FROM generate_series(0, earliest_forecast_offset_mins - forecast_resolution_mins, forecast_resolution_mins) AS i;
        END LOOP; 

        -- Insert observed generation values covering all the forecast period, always half the capacity
        INSERT INTO obs.observed_generation_values
            (value_sip, source_type_id, observer_uuid, geometry_uuid, observation_timestamp_utc)
        SELECT
            15000::SMALLINT,
            1,
            (SELECT observer_uuid FROM obs.observers WHERE observer_name = LOWER(name_prefix) || '_observer'),
            geo_id,
            pivot_time - (i || ' minutes')::interval
        FROM generate_series(0, earliest_forecast_offset_mins - gv_resolution_mins, gv_resolution_mins) AS i;

    END LOOP;
    RAISE NOTICE 'Inserted % observed generation values', (SELECT COUNT(*) FROM obs.observed_generation_values);

    -- Insert predicted generation values for each forecast
    FOR result IN SELECT forecast_uuid, init_time_utc FROM pred.forecasts LOOP
        INSERT INTO pred.predicted_generation_values
            (horizon_mins, p50_sip, forecast_uuid, target_time_utc, metadata, other_stats_fractions)
        SELECT
            i,
            CAST((100 / num_pgvs_per_forecast) * (i / gv_resolution_mins) * (30000/100) AS SMALLINT),
            result.forecast_uuid,
            result.init_time_utc + (i || ' minutes')::interval,
            jsonb_build_object('source', 'test'),
            jsonb_build_object(
                'p10_fraction', TRUNC(GREATEST(((100 / num_pgvs_per_forecast) * (i / gv_resolution_mins) / 100) - 0.03, 0), 3),
                'p90_fraction', TRUNC(((100 / num_pgvs_per_forecast) * (i / gv_resolution_mins) / 100) + 0.03, 3)
            )
        FROM generate_series(0, forecast_length_mins - gv_resolution_mins, gv_resolution_mins) AS i;
    END LOOP;
    RAISE NOTICE 'Inserted % predicted generation values', (SELECT COUNT(*) FROM pred.predicted_generation_values);

    REFRESH MATERIALIZED VIEW CONCURRENTLY loc.sources_mv;

    RETURN QUERY
    SELECT
        (SELECT COUNT(*) FROM pred.predicted_generation_values)::INTEGER,
        (SELECT ARRAY_AGG(geometry_uuid) FROM loc.geometries);

END;
$$ LANGUAGE plpgsql;



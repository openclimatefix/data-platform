CREATE OR REPLACE FUNCTION seed_db(
    name_prefix TEXT DEFAULT '',
    target_total_forecasts INTEGER DEFAULT 1000, 
    pivot_time TIMESTAMP DEFAULT DATE_TRUNC('hour', NOW())
)
RETURNS TABLE (num_values INTEGER, geometry_uuids UUID[]) AS $$
DECLARE
    -- Constants
    num_locations CONSTANT INTEGER := 1000;
    forecast_freq_mins CONSTANT INTEGER := 15;
    forecast_len_mins CONSTANT INTEGER := 720; -- 12 hours
    pgv_res_mins CONSTANT INTEGER := 30;
    history_window_mins CONSTANT INTEGER := 10080; -- 1 week
    
    -- Derived values
    forecasts_per_loc INTEGER;
    num_forecasters INTEGER;
    geo_list UUID[];
    o_uuid UUID;
BEGIN
    -- Should speed up inserts
    EXECUTE 'SET LOCAL work_mem = ''128MB''';
    EXECUTE 'SET LOCAL enable_seqscan = off';

    forecasts_per_loc := target_total_forecasts / num_locations;
    num_forecasters := GREATEST(1, forecasts_per_loc / (history_window_mins / forecast_freq_mins));

    -- Insert Forecasters and Observers
    INSERT INTO pred.forecasters (forecaster_name, forecaster_version)
    SELECT name_prefix || '_forecaster_' || i, 'v1'
    FROM generate_series(1, num_forecasters) i
    ON CONFLICT DO NOTHING;

    INSERT INTO obs.observers (observer_name) 
    VALUES (name_prefix || '_observer')
    ON CONFLICT DO NOTHING
    RETURNING observer_uuid INTO o_uuid;

    -- Insert Geometries and Sources
    WITH inserted_geos AS (
        INSERT INTO loc.geometries (geometry_name, geometry_type_id, geom, associated_point)
        SELECT name_prefix || '_location_' || i, 1,
            ST_SetSRID(ST_MakePoint(0,0), 4326), ST_SetSRID(ST_MakePoint(0,0), 4326)
        FROM generate_series(1, num_locations) i
        RETURNING geometry_uuid
    )
    SELECT array_agg(geometry_uuid) FROM inserted_geos INTO geo_list;

    INSERT INTO loc.sources_history (geometry_uuid, source_type_id, capacity_watts, valid_from_utc)
    SELECT u, 1, 5000, pivot_time - INTERVAL '10 years'
    FROM unnest(geo_list) u;

    -- Insert observations
    INSERT INTO obs.observed_generation_values 
        (value_sip, source_type_id, observer_uuid, geometry_uuid, observation_timestamp_utc)
    SELECT (random() * 30000)::SMALLINT, 1, o_uuid, u, pivot_time - (s.idx * INTERVAL '30 minutes')
    FROM unnest(geo_list) u, generate_series(0, 100) AS s(idx);

    -- Insert all forecasts at once
    WITH inserted_forecasts AS (
        INSERT INTO pred.forecasts 
            (forecast_uuid, source_type_id, geometry_uuid, forecaster_id, init_time_utc, value_resolution_mins, target_period)
        SELECT UUIDV7(pivot_time - (s.idx * INTERVAL '15 minutes')), 1, u.geo_id, f.forecaster_id,
            pivot_time - (s.idx * INTERVAL '15 minutes'),
            pgv_res_mins::SMALLINT,
            TSRANGE(pivot_time - (s.idx * INTERVAL '15 minutes'),
                    pivot_time - (s.idx * INTERVAL '15 minutes') + (forecast_len_mins * INTERVAL '1 minute'))
        FROM unnest(geo_list) AS u(geo_id)
        CROSS JOIN (SELECT forecaster_id FROM pred.forecasters WHERE forecaster_name LIKE name_prefix || '%') f
        CROSS JOIN generate_series(0, (history_window_mins / forecast_freq_mins) - 1) AS s(idx)
        RETURNING forecast_uuid, init_time_utc
    )
    -- Use inserted forecasts to create predicted generation values
    INSERT INTO pred.predicted_generation_values 
        (horizon_mins, p50_sip, forecast_uuid, target_time_utc, metadata, other_stats_fractions)
    SELECT gs.h, (random() * 30000)::SMALLINT, inf.forecast_uuid, inf.init_time_utc + (gs.h * INTERVAL '1 minute'),
        '{"source": "benchmark"}'::jsonb, '{"p10": 0.1, "p90": 0.9}'::jsonb
    FROM inserted_forecasts inf
    CROSS JOIN LATERAL generate_series(0, forecast_len_mins - pgv_res_mins, pgv_res_mins) AS gs(h);

    -- Spoof the table size so indexes used in queries reflect production
    UPDATE pg_class SET reltuples = 346000000, relpages = 5000000 WHERE relname = 'predicted_generation_values';
    REFRESH MATERIALIZED VIEW loc.sources_mv;
    
    RETURN QUERY SELECT target_total_forecasts * (forecast_len_mins / pgv_res_mins), geo_list;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION seed_db(
    name_prefix TEXT DEFAULT '',
    target_locations INTEGER DEFAULT 100,
    history_window_mins INTEGER DEFAULT 2880,
    pivot_time TIMESTAMP DEFAULT DATE_TRUNC('hour', NOW())
)
RETURNS TABLE (num_values INTEGER, geometry_uuids UUID[]) AS $$
DECLARE
    -- Constants
    forecast_freq_mins CONSTANT INTEGER := 15;
    forecast_len_mins CONSTANT INTEGER := 720; 
    pgv_res_mins CONSTANT INTEGER := 30;
    
    geo_list UUID[];
    o_uuid UUID;
BEGIN
    EXECUTE 'SET LOCAL work_mem = ''128MB''';
    EXECUTE 'SET LOCAL enable_seqscan = off';

    INSERT INTO pred.forecasters (forecaster_name, forecaster_version)
    VALUES (name_prefix || '_forecaster_1', 'v1')
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
        FROM generate_series(1, target_locations) i
        RETURNING geometry_uuid
    )
    SELECT array_agg(geometry_uuid) FROM inserted_geos INTO geo_list;

    INSERT INTO loc.sources_history (geometry_uuid, source_type_id, capacity_watts, valid_from_utc)
    SELECT u, 1, 5000, pivot_time - INTERVAL '10 years'
    FROM unnest(geo_list) u;

    -- Insert observations for the past 36 hours at 30-minute intervals (72 data points per location)
    INSERT INTO obs.observed_generation_values 
        (value_sip, source_type_id, observer_uuid, geometry_uuid, observation_timestamp_utc)
    SELECT (random() * 30000)::SMALLINT, 1, o_uuid, u, pivot_time - (s.idx * INTERVAL '30 minutes')
    FROM unnest(geo_list) u, generate_series(0, 72) AS s(idx);

    -- Insert Forecasts 
    WITH generated_data AS (
        SELECT 
            u.geo_id, 
            pivot_time - (s.idx * INTERVAL '15 minutes') AS init_time_utc
        FROM unnest(geo_list) AS u(geo_id)
        CROSS JOIN generate_series(0, (history_window_mins / forecast_freq_mins) - 1) AS s(idx)
        ORDER BY init_time_utc ASC
    ),
    inserted_forecasts AS (
        INSERT INTO pred.forecasts 
            (forecast_uuid, source_type_id, geometry_uuid, forecaster_id, init_time_utc, value_resolution_mins, target_period)
        SELECT 
            UUIDV7(init_time_utc), 1, geo_id, 
            (SELECT forecaster_id FROM pred.forecasters WHERE forecaster_name = name_prefix || '_forecaster_1'),
            init_time_utc, pgv_res_mins::SMALLINT,
            TSRANGE(init_time_utc, init_time_utc + (forecast_len_mins * INTERVAL '1 minute'))
        FROM generated_data
        RETURNING forecast_uuid, init_time_utc
    ),
    static_json AS (
        -- Removed the stats JSON payload entirely to mirror the dropped column
        SELECT '{"source": "benchmark"}'::jsonb AS meta
    )
    INSERT INTO pred.predicted_generation_values 
        (horizon_mins, p50_sip, p10_sip, p90_sip, forecast_uuid, metadata)
    SELECT 
        gs.h, 
        (random() * 30000)::SMALLINT,
        3000::SMALLINT,
        27000::SMALLINT,
        inf.forecast_uuid, 
        sj.meta
    FROM inserted_forecasts inf
    CROSS JOIN static_json sj
    CROSS JOIN LATERAL generate_series(0, forecast_len_mins - pgv_res_mins, pgv_res_mins) AS gs(h)
    ORDER BY inf.init_time_utc ASC;

    -- Spoof the table size so Postgres uses indexes rather than seq scan in testing
    UPDATE pg_class SET reltuples = 346000000, relpages = 5000000 WHERE relname = 'predicted_generation_values';
    UPDATE pg_class SET reltuples = 346000000, relpages = 5000000 WHERE relname = 'predicted_generation_values_pkey';
    
    REFRESH MATERIALIZED VIEW loc.sources_mv;
    
    RETURN QUERY SELECT target_locations * (history_window_mins / forecast_freq_mins) * (forecast_len_mins / pgv_res_mins), geo_list;
END;
$$ LANGUAGE plpgsql;

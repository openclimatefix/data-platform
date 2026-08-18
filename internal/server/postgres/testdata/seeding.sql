CREATE OR REPLACE FUNCTION spoof_uuidv7(ts timestamptz) RETURNS uuid AS $$
SELECT encode(
   set_bit(
	   set_bit(
		   overlay(uuid_send(gen_random_uuid()) placing
		   substring(int8send(floor(extract(epoch from ts) * 1000)::bigint) from 3)
		   from 1 for 6),
	   52, 1),
   53, 1),
'hex')::uuid;
$$ LANGUAGE sql volatile;

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
    )
    INSERT INTO pred.forecasts 
        (forecast_uuid, source_type_id, geometry_uuid, forecaster_id, init_time_utc, value_resolution_mins, target_period,
         p50_sips, p10_sips, p90_sips)
    SELECT 
        SPOOF_UUIDV7(gd.init_time_utc AT TIME ZONE 'UTC'), 1, gd.geo_id, 
        (SELECT forecaster_id FROM pred.forecasters WHERE forecaster_name = name_prefix || '_forecaster_1'),
        gd.init_time_utc, pgv_res_mins::SMALLINT,
        TSRANGE(gd.init_time_utc, gd.init_time_utc + (forecast_len_mins * INTERVAL '1 minute')),
        v.p50_sips,
        array_fill(3000::SMALLINT, ARRAY[forecast_len_mins / pgv_res_mins]),
        array_fill(27000::SMALLINT, ARRAY[forecast_len_mins / pgv_res_mins])
    FROM generated_data gd
    -- Correlated on init_time_utc so each forecast gets its own values; an uncorrelated
    -- subquery is an InitPlan, evaluated once, giving every forecast the same array.
    CROSS JOIN LATERAL (
        SELECT array_agg((random() * 30000)::SMALLINT) AS p50_sips
        FROM generate_series(
            gd.init_time_utc,
            gd.init_time_utc + ((forecast_len_mins - pgv_res_mins) * INTERVAL '1 minute'),
            (pgv_res_mins * INTERVAL '1 minute')
        )
    ) AS v;

    -- Spoof the table size so Postgres uses indexes rather than seq scan in testing
    UPDATE pg_class SET reltuples = 14400000, relpages = 1600000 WHERE relname = 'forecasts';
    UPDATE pg_class SET reltuples = 14400000, relpages = 1600000 WHERE relname = 'idx_forecasts_filter';
    
    REFRESH MATERIALIZED VIEW loc.sources_mv;
    
    RETURN QUERY SELECT target_locations * (history_window_mins / forecast_freq_mins) * (forecast_len_mins / pgv_res_mins), geo_list;
END;
$$ LANGUAGE plpgsql;

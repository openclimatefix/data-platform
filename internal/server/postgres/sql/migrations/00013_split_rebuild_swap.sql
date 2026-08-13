-- +goose Up

-- +goose StatementBegin
/*
 * Splits pred.rebuild_forecast_partition into a build phase and a swap phase.
 */

CREATE OR REPLACE PROCEDURE pred.build_forecast_partition(
       p_partition TEXT,
       p_chunk INTERVAL DEFAULT INTERVAL '1 hour',
       p_work_mem TEXT DEFAULT '256MB',
       p_batch_rows BIGINT DEFAULT 50000
   )
   LANGUAGE plpgsql AS $$
   DECLARE
       v_values    TEXT;
       v_new       TEXT := p_partition || '_v2';
       v_bounds    TEXT;
       v_default   BOOLEAN;
       v_lo        TIMESTAMP;
       v_hi        TIMESTAMP;
       v_t         TIMESTAMP;
       v_last_uuid UUID;
       v_next_uuid UUID;
       v_n         BIGINT;
       v_total     BIGINT := 0;
       v_all       BIGINT;
       v_src       BIGINT;
       v_dst       BIGINT;
       v_started   TIMESTAMPTZ := clock_timestamp();
   BEGIN
       SELECT pg_get_expr(c.relpartbound, c.oid) INTO v_bounds
       FROM pg_class AS c INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
       WHERE n.nspname = 'pred' AND c.relname = p_partition;

       IF v_bounds IS NULL THEN
           RAISE EXCEPTION 'not an attached partition of pred.forecasts: pred.%', p_partition;
       END IF;

       v_default := (v_bounds = 'DEFAULT');

       /* Sibling naming is the same prefix swap for every partition, including the default one:
        * forecasts_pXXXXXXXX -> predicted_generation_values_pXXXXXXXX,
        * forecasts_default    -> predicted_generation_values_default. */
       v_values := 'predicted_generation_values_' || substring(p_partition FROM '^forecasts_(.+)$');

       IF v_values IS NULL OR to_regclass('pred.' || quote_ident(v_values)) IS NULL THEN
           RAISE EXCEPTION 'no values partition matching pred.%: expected pred.%',
               p_partition, v_values;
       END IF;

       IF to_regclass('pred.' || quote_ident(v_new)) IS NOT NULL THEN
           RAISE EXCEPTION 'pred.% already exists',
               v_new
           USING HINT = format(
               'CALL pred.swap_forecast_partition(%L) to finish that run, or DROP TABLE pred.%I to start over',
               p_partition, v_new);
       END IF;

       CREATE UNLOGGED TABLE IF NOT EXISTS pred.fc_staging (
           forecast_uuid UUID PRIMARY KEY,
           p02_sips SMALLINT [], p10_sips SMALLINT [], p25_sips SMALLINT [],
           p50_sips SMALLINT [], p75_sips SMALLINT [], p90_sips SMALLINT [],
           p98_sips SMALLINT []
       );
       TRUNCATE pred.fc_staging;

       IF v_default THEN
           /* No time bounds to chunk over - forecast_uuid in this partition can be anything that
            * fell outside every managed weekly range. Chunk by distinct forecast_uuid instead:
            * find the batch boundary first, then aggregate up to it, so a forecast's horizon rows
            * never straddle two batches. */
           v_last_uuid := '00000000-0000-0000-0000-000000000000'::UUID;

           LOOP
               EXECUTE format($q$
                   SELECT forecast_uuid FROM (
                       SELECT DISTINCT forecast_uuid FROM pred.%I
                       WHERE forecast_uuid > %L
                       ORDER BY forecast_uuid
                       LIMIT %L
                   ) AS batch ORDER BY forecast_uuid DESC LIMIT 1
               $q$, v_values, v_last_uuid, p_batch_rows) INTO v_next_uuid;

               EXIT WHEN v_next_uuid IS NULL;

               EXECUTE format($q$
                   INSERT INTO pred.fc_staging (
                       forecast_uuid, p02_sips, p10_sips, p25_sips,
                       p50_sips, p75_sips, p90_sips, p98_sips)
                   SELECT
                       forecast_uuid,
                       CASE WHEN bool_or(p02_sip IS NOT NULL) THEN array_agg(p02_sip ORDER BY horizon_mins) END,
                       CASE WHEN bool_or(p10_sip IS NOT NULL) THEN array_agg(p10_sip ORDER BY horizon_mins) END,
                       CASE WHEN bool_or(p25_sip IS NOT NULL) THEN array_agg(p25_sip ORDER BY horizon_mins) END,
                       array_agg(p50_sip ORDER BY horizon_mins),
                       CASE WHEN bool_or(p75_sip IS NOT NULL) THEN array_agg(p75_sip ORDER BY horizon_mins) END,
                       CASE WHEN bool_or(p90_sip IS NOT NULL) THEN array_agg(p90_sip ORDER BY horizon_mins) END,
                       CASE WHEN bool_or(p98_sip IS NOT NULL) THEN array_agg(p98_sip ORDER BY horizon_mins) END
                   FROM pred.%I
                   WHERE forecast_uuid > %L AND forecast_uuid <= %L
                   GROUP BY forecast_uuid
                   ON CONFLICT (forecast_uuid) DO NOTHING
               $q$, v_values, v_last_uuid, v_next_uuid);

               GET DIAGNOSTICS v_n = ROW_COUNT;
               v_total := v_total + v_n;
               v_last_uuid := v_next_uuid;

               COMMIT;

               RAISE NOTICE '...  %  +% (total %, elapsed %)',
                   v_last_uuid, v_n, v_total, clock_timestamp() - v_started;
           END LOOP;
       ELSE
           v_lo := partman.uuid7_time_decoder(
               (regexp_match(v_bounds, $re$FROM \('([^']+)'\)$re$))[1]::UUID::TEXT) AT TIME ZONE 'UTC';
           v_hi := partman.uuid7_time_decoder(
               (regexp_match(v_bounds, $re$TO \('([^']+)'\)$re$))[1]::UUID::TEXT) AT TIME ZONE 'UTC';

           RAISE NOTICE 'pred.% covers % .. % (% chunks of %)',
               p_partition, v_lo, v_hi,
               CEIL(EXTRACT(EPOCH FROM (v_hi - v_lo)) / EXTRACT(EPOCH FROM p_chunk)), p_chunk;

           v_t := v_lo;
           WHILE v_t < v_hi LOOP
               EXECUTE format($q$
                   INSERT INTO pred.fc_staging (
                       forecast_uuid, p02_sips, p10_sips, p25_sips,
                       p50_sips, p75_sips, p90_sips, p98_sips)
                   SELECT
                       forecast_uuid,
                       CASE WHEN bool_or(p02_sip IS NOT NULL) THEN array_agg(p02_sip ORDER BY horizon_mins) END,
                       CASE WHEN bool_or(p10_sip IS NOT NULL) THEN array_agg(p10_sip ORDER BY horizon_mins) END,
                       CASE WHEN bool_or(p25_sip IS NOT NULL) THEN array_agg(p25_sip ORDER BY horizon_mins) END,
                       array_agg(p50_sip ORDER BY horizon_mins),
                       CASE WHEN bool_or(p75_sip IS NOT NULL) THEN array_agg(p75_sip ORDER BY horizon_mins) END,
                       CASE WHEN bool_or(p90_sip IS NOT NULL) THEN array_agg(p90_sip ORDER BY horizon_mins) END,
                       CASE WHEN bool_or(p98_sip IS NOT NULL) THEN array_agg(p98_sip ORDER BY horizon_mins) END
                   FROM pred.%I
                   WHERE forecast_uuid >= uuidv7_boundary(%L::TIMESTAMP AT TIME ZONE 'UTC')
                     AND forecast_uuid <  uuidv7_boundary(%L::TIMESTAMP AT TIME ZONE 'UTC')
                   GROUP BY forecast_uuid
                   ON CONFLICT (forecast_uuid) DO NOTHING
               $q$, v_values, v_t, v_t + p_chunk);

               GET DIAGNOSTICS v_n = ROW_COUNT;
               v_total := v_total + v_n;

               COMMIT;

               RAISE NOTICE '% .. %  +% (total %, elapsed %)',
                   v_t, v_t + p_chunk, v_n, v_total, clock_timestamp() - v_started;

               v_t := v_t + p_chunk;
           END LOOP;
       END IF;

       RAISE NOTICE 'staged % forecasts in %', v_total, clock_timestamp() - v_started;

       EXECUTE format('SET LOCAL work_mem = %L', p_work_mem);

       EXECUTE format('CREATE TABLE pred.%I (LIKE pred.forecasts INCLUDING ALL)', v_new);

       /* LEFT JOIN rather than INNER, with COALESCE onto the source row's own arrays: a forecast
        * that already has arrays (written after the array-write deploy landed) has no row in
        * fc_staging and must not be dropped. Only a forecast with neither a staging match nor its
        * own arrays is truly value-less and gets excluded by the WHERE below. */
       EXECUTE format($q$
           INSERT INTO pred.%I (
               forecast_uuid, geometry_uuid, source_type_id, forecaster_id, init_time_utc,
               value_resolution_mins, target_period, metadata, created_at_utc,
               p02_sips, p10_sips, p25_sips, p50_sips, p75_sips, p90_sips, p98_sips
           )
           SELECT f.forecast_uuid, f.geometry_uuid, f.source_type_id, f.forecaster_id, f.init_time_utc,
                  f.value_resolution_mins, f.target_period, f.metadata, f.created_at_utc,
                  COALESCE(s.p02_sips, f.p02_sips), COALESCE(s.p10_sips, f.p10_sips),
                  COALESCE(s.p25_sips, f.p25_sips), COALESCE(s.p50_sips, f.p50_sips),
                  COALESCE(s.p75_sips, f.p75_sips), COALESCE(s.p90_sips, f.p90_sips),
                  COALESCE(s.p98_sips, f.p98_sips)
           FROM pred.%I AS f
           LEFT JOIN pred.fc_staging AS s USING (forecast_uuid)
           WHERE s.forecast_uuid IS NOT NULL OR f.p50_sips IS NOT NULL
           ORDER BY f.geometry_uuid, f.source_type_id, f.forecaster_id, f.forecast_uuid DESC
       $q$, v_new, p_partition);

       EXECUTE format('SELECT count(*) FROM pred.%I', p_partition) INTO v_all;
       EXECUTE format(
           'SELECT count(*) FROM pred.%I AS f
            WHERE f.p50_sips IS NOT NULL
               OR EXISTS (SELECT 1 FROM pred.%I AS v WHERE v.forecast_uuid = f.forecast_uuid)',
           p_partition, v_values) INTO v_src;
       EXECUTE format('SELECT count(*) FROM pred.%I', v_new) INTO v_dst;

       IF v_src <> v_dst THEN
           RAISE EXCEPTION 'row count mismatch for %: % source forecasts with values -> % rebuilt rows',
               p_partition, v_src, v_dst;
       END IF;

       EXECUTE format(
           'ALTER TABLE pred.%I ADD CONSTRAINT p50_sips_not_null CHECK (p50_sips IS NOT NULL)', v_new);

       EXECUTE format('ANALYZE pred.%I', v_new);

       DROP TABLE pred.fc_staging;

       RAISE NOTICE 'built pred.%: % rows (% forecasts had no values and were dropped) in %',
           v_new, v_dst, v_all - v_dst, clock_timestamp() - v_started;
       RAISE NOTICE 'nothing is swapped yet: CALL pred.swap_forecast_partition(''%'')', p_partition;
   END;
   $$;
-- +goose StatementEnd

-- +goose StatementBegin
/*
 * Swaps a table built by pred.build_forecast_partition in for its partition.
 */
CREATE OR REPLACE PROCEDURE pred.swap_forecast_partition(
       p_partition TEXT,
       p_lock_timeout TEXT DEFAULT '5s'
   )
   LANGUAGE plpgsql AS $$
   DECLARE
       v_values  TEXT;
       v_new     TEXT := p_partition || '_v2';
       v_old     TEXT := p_partition || '_retired';
       v_bounds  TEXT;
       v_fk      TEXT;
       v_names   JSONB;
       v_target  TEXT;
       r         RECORD;
       v_started TIMESTAMPTZ := clock_timestamp();
   BEGIN
       SELECT pg_get_expr(c.relpartbound, c.oid) INTO v_bounds
       FROM pg_class AS c INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
       WHERE n.nspname = 'pred' AND c.relname = p_partition;

       IF v_bounds IS NULL THEN
           RAISE EXCEPTION 'not an attached partition of pred.forecasts: pred.%', p_partition;
       END IF;

       IF to_regclass('pred.' || quote_ident(v_new)) IS NULL THEN
           RAISE EXCEPTION 'no rebuilt table pred.%', v_new
           USING HINT = format('CALL pred.build_forecast_partition(%L) first', p_partition);
       END IF;

       v_values := 'predicted_generation_values_' || substring(p_partition FROM '^forecasts_(.+)$');

       IF v_values IS NULL OR to_regclass('pred.' || quote_ident(v_values)) IS NULL THEN
           RAISE EXCEPTION 'no values partition matching pred.%: expected pred.%',
               p_partition, v_values;
       END IF;

       EXECUTE format('SET LOCAL lock_timeout = %L', p_lock_timeout);
       LOCK TABLE pred.forecasts IN ACCESS EXCLUSIVE MODE;
       LOCK TABLE pred.predicted_generation_values IN ACCESS EXCLUSIVE MODE;

       EXECUTE format(
           'ALTER TABLE pred.predicted_generation_values DETACH PARTITION pred.%I', v_values);

       SELECT conname INTO v_fk
       FROM pg_constraint
       WHERE conrelid = ('pred.' || quote_ident(v_values))::regclass
           AND contype = 'f'
           AND confrelid = 'pred.forecasts'::regclass;

       IF v_fk IS NOT NULL THEN
           EXECUTE format('ALTER TABLE pred.%I DROP CONSTRAINT %I', v_values, v_fk);
       END IF;

       EXECUTE format('ALTER TABLE pred.%I RENAME TO %I', v_values, v_values || '_retired');

       EXECUTE format('ALTER TABLE pred.forecasts DETACH PARTITION pred.%I', p_partition);
       EXECUTE format('ALTER TABLE pred.%I RENAME TO %I', p_partition, v_old);
       EXECUTE format('ALTER TABLE pred.forecasts ATTACH PARTITION pred.%I %s', v_new, v_bounds);
       EXECUTE format('ALTER TABLE pred.%I RENAME TO %I', v_new, p_partition);

       SELECT jsonb_object_agg(s.definition, s.index_name) INTO v_names
       FROM (
           SELECT regexp_replace(pg_get_indexdef(i.indexrelid),
                                 '^CREATE (UNIQUE )?INDEX \S+ ON \S+ ', '') AS definition,
                  c.relname AS index_name
           FROM pg_index AS i INNER JOIN pg_class AS c ON c.oid = i.indexrelid
           WHERE i.indrelid = ('pred.' || quote_ident(v_old))::REGCLASS
       ) AS s;

       FOR r IN
           SELECT c.oid, c.relname
           FROM pg_index AS i INNER JOIN pg_class AS c ON c.oid = i.indexrelid
           WHERE i.indrelid = ('pred.' || quote_ident(v_old))::REGCLASS
       LOOP
           EXECUTE format('ALTER INDEX pred.%I RENAME TO %I', r.relname, 'retired_' || r.oid);
       END LOOP;

       FOR r IN
           SELECT c.relname,
                  regexp_replace(pg_get_indexdef(i.indexrelid),
                                 '^CREATE (UNIQUE )?INDEX \S+ ON \S+ ', '') AS definition
           FROM pg_index AS i INNER JOIN pg_class AS c ON c.oid = i.indexrelid
           WHERE i.indrelid = ('pred.' || quote_ident(p_partition))::REGCLASS
       LOOP
           v_target := COALESCE(v_names ->> r.definition, replace(r.relname, v_new, p_partition));

           IF v_target <> r.relname THEN
               EXECUTE format('ALTER INDEX pred.%I RENAME TO %I', r.relname, v_target);
           END IF;
       END LOOP;

       RAISE NOTICE 'swapped % in % (old rows retained as pred.%_retired and pred.%_retired)',
           p_partition, clock_timestamp() - v_started, p_partition, v_values;
   END;
   $$;
-- +goose StatementEnd

-- +goose StatementBegin
/* Both phases in one call, for a partition being rebuilt start to finish. If the swap loses the
 * lock race, retry it alone - the build is committed and does not need repeating. */
CREATE OR REPLACE PROCEDURE pred.rebuild_forecast_partition(
    p_partition TEXT,
    p_chunk INTERVAL DEFAULT INTERVAL '1 hour',
    p_work_mem TEXT DEFAULT '256MB'
)
LANGUAGE plpgsql AS $$
BEGIN
    CALL pred.build_forecast_partition(p_partition, p_chunk, p_work_mem);
    CALL pred.swap_forecast_partition(p_partition);
END;
$$;
-- +goose StatementEnd

-- +goose Down
/* 00012's Down drops rebuild_forecast_partition too, so rolling back past this leaves no
 * rebuild procedure rather than restoring the version that deadlocks. */
DROP PROCEDURE IF EXISTS pred.rebuild_forecast_partition(TEXT, INTERVAL, TEXT);
DROP PROCEDURE IF EXISTS pred.swap_forecast_partition(TEXT, TEXT);
DROP PROCEDURE IF EXISTS pred.build_forecast_partition(TEXT, INTERVAL, TEXT);

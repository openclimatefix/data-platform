-- +goose Up

-- +goose StatementBegin
/*
 * Rebuilds one pred.forecasts partition with its values folded into arrays, and its rows
 * physically ordered to match idx_forecasts_filter.
 *
 * A rebuild rather than an UPDATE, because rows grow ~3.6x: an in-place update cannot keep the
 * new tuple on its page, so every row would be a non-HOT update leaving a dead tuple and a new
 * entry in every index. Rebuilding also lets us choose physical order for free, and packs
 * indexes at full density.
 *
 * Ordering by (geometry_uuid, source_type_id, forecaster_id, forecast_uuid DESC) makes one
 * location's forecasts contiguous and matches idx_forecasts_filter, so an index scan walks the
 * heap in physical order. Every hot-path query filters on geometry_uuid first, so nothing loses.
 * StreamForecastData is the only broad scan and is explicitly rare.
 *
 * The aggregation is chunked into an unlogged staging table and committed per chunk, so a week's
 * worth of values is never sorted in one go.
 *
 * Forecasts with no rows in the values partition are dropped: the INNER JOIN against staging
 * excludes them, and the row count check below is written to expect that.
 *
 * pred.predicted_generation_values carries a foreign key to pred.forecasts, and PostgreSQL
 * refuses to detach a partition that is still referenced. The matching values partition is
 * therefore detached and retired in the same transaction as the swap - which is the correct
 * coupling anyway, since once a week's forecasts hold arrays its value rows are dead. It is
 * renamed rather than dropped so the rebuild stays verifiable and reversible. The parent's
 * foreign key is left intact for every partition that has not yet been rebuilt.
 *
 * Retired tables are left on disk as pred.predicted_generation_values_pXXXXXXXX_retired. They
 * are no longer partitions, so the cleanup deployment's DROP TABLE will not remove them - drop
 * them explicitly once the rebuild has been verified.
 *
 * This must be driven one partition at a time rather than looped unattended: DETACH and ATTACH
 * each take a brief ACCESS EXCLUSIVE lock on pred.forecasts, and ATTACH validates the partition
 * bound and the foreign keys.
 *
 *     CALL pred.rebuild_forecast_partition('forecasts_p20260803')
 */
CREATE OR REPLACE PROCEDURE pred.rebuild_forecast_partition(
    p_partition TEXT,
    p_chunk INTERVAL DEFAULT INTERVAL '1 hour',
    p_work_mem TEXT DEFAULT '256MB'
)
LANGUAGE plpgsql AS $$
DECLARE
    v_values  TEXT;
    v_fk      TEXT;
    v_new     TEXT := p_partition || '_v2';
    v_bounds  TEXT;
    v_lo      TIMESTAMP;
    v_hi      TIMESTAMP;
    v_t       TIMESTAMP;
    v_n       BIGINT;
    v_total   BIGINT := 0;
    v_all     BIGINT;
    v_src     BIGINT;
    v_dst     BIGINT;
    v_started TIMESTAMPTZ := clock_timestamp();
BEGIN
    SELECT pg_get_expr(c.relpartbound, c.oid) INTO v_bounds
    FROM pg_class AS c INNER JOIN pg_namespace AS n ON n.oid = c.relnamespace
    WHERE n.nspname = 'pred' AND c.relname = p_partition;

    IF v_bounds IS NULL THEN
        RAISE EXCEPTION 'not an attached partition of pred.forecasts: pred.%', p_partition;
    END IF;

    /* pg_partman names siblings <table>_p<suffix>, so the values partition covering the same
     * uuid range differs only in its prefix. */
    v_values := 'predicted_generation_values_' || substring(p_partition FROM '^forecasts_(p.+)$');

    IF v_values IS NULL OR to_regclass('pred.' || quote_ident(v_values)) IS NULL THEN
        RAISE EXCEPTION 'no values partition matching pred.%: expected pred.%',
            p_partition, v_values;
    END IF;

    v_lo := partman.uuid7_time_decoder(
        (regexp_match(v_bounds, $re$FROM \('([^']+)'\)$re$))[1]::UUID) AT TIME ZONE 'UTC';
    v_hi := partman.uuid7_time_decoder(
        (regexp_match(v_bounds, $re$TO \('([^']+)'\)$re$))[1]::UUID) AT TIME ZONE 'UTC';

    RAISE NOTICE 'pred.% covers % .. % (% chunks of %)',
        p_partition, v_lo, v_hi,
        CEIL(EXTRACT(EPOCH FROM (v_hi - v_lo)) / EXTRACT(EPOCH FROM p_chunk)), p_chunk;

    CREATE UNLOGGED TABLE IF NOT EXISTS pred.fc_staging (
        forecast_uuid UUID PRIMARY KEY,
        p02_sips SMALLINT [], p10_sips SMALLINT [], p25_sips SMALLINT [],
        p50_sips SMALLINT [], p75_sips SMALLINT [], p90_sips SMALLINT [],
        p98_sips SMALLINT []
    );

    /* An earlier run that failed after filling would otherwise leave rows behind that the
     * ON CONFLICT DO NOTHING below would silently keep. */
    TRUNCATE pred.fc_staging;

    v_t := v_lo;
    WHILE v_t < v_hi LOOP
        /* The CASE WHEN bool_or(...) guard keeps an unused p-level as a NULL array rather than a
         * materialised array of nulls: ~3 bytes per forecast against ~130. */
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

    RAISE NOTICE 'staged % forecasts in %', v_total, clock_timestamp() - v_started;

    /* Reverts on commit of the transaction the swap runs in. */
    EXECUTE format('SET LOCAL work_mem = %L', p_work_mem);

    EXECUTE format('CREATE TABLE pred.%I (LIKE pred.forecasts INCLUDING ALL)', v_new);

    EXECUTE format($q$
        INSERT INTO pred.%I (
            forecast_uuid, geometry_uuid, source_type_id, forecaster_id, init_time_utc,
            value_resolution_mins, target_period, metadata, created_at_utc,
            p02_sips, p10_sips, p25_sips, p50_sips, p75_sips, p90_sips, p98_sips
        )
        SELECT f.forecast_uuid, f.geometry_uuid, f.source_type_id, f.forecaster_id, f.init_time_utc,
               f.value_resolution_mins, f.target_period, f.metadata, f.created_at_utc,
               s.p02_sips, s.p10_sips, s.p25_sips, s.p50_sips, s.p75_sips, s.p90_sips, s.p98_sips
        FROM pred.%I AS f
        INNER JOIN pred.fc_staging AS s USING (forecast_uuid)
        ORDER BY f.geometry_uuid, f.source_type_id, f.forecaster_id, f.forecast_uuid DESC
    $q$, v_new, p_partition);

    EXECUTE format('SELECT count(*) FROM pred.%I', p_partition) INTO v_all;
    EXECUTE format(
        'SELECT count(*) FROM pred.%I AS f
         WHERE EXISTS (SELECT 1 FROM pred.%I AS v WHERE v.forecast_uuid = f.forecast_uuid)',
        p_partition, v_values) INTO v_src;
    EXECUTE format('SELECT count(*) FROM pred.%I', v_new) INTO v_dst;

    IF v_src <> v_dst THEN
        RAISE EXCEPTION 'row count mismatch for %: % source forecasts with values -> % rebuilt rows',
            p_partition, v_src, v_dst;
    END IF;

    RAISE NOTICE 'rebuilt %: % rows (% forecasts had no values and were dropped)',
        p_partition, v_dst, v_all - v_dst;

    /* Marks the partition as migrated. Note this does not buy constraint exclusion on the read
     * queries' legacy branch: they filter p50_sips inside a CTE rather than on a direct scan of
     * pred.forecasts, so the planner cannot use it to prune. It is an integrity check and an
     * operational marker for which partitions are done. Added before ATTACH so the fresh,
     * exclusively-locked table is scanned rather than a live partition. */
    EXECUTE format(
        'ALTER TABLE pred.%I ADD CONSTRAINT migrated_check CHECK (p50_sips IS NOT NULL)', v_new);

    EXECUTE format('ANALYZE pred.%I', v_new);

    /* Detaching leaves a standalone copy of the foreign key behind on the values partition, which
     * would still block the forecasts detach below, so it has to go too. */
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
    EXECUTE format('ALTER TABLE pred.forecasts ATTACH PARTITION pred.%I %s', v_new, v_bounds);
    EXECUTE format('DROP TABLE pred.%I', p_partition);
    EXECUTE format('ALTER TABLE pred.%I RENAME TO %I', v_new, p_partition);

    DROP TABLE pred.fc_staging;

    RAISE NOTICE 'done: % in % (old values retained as pred.%_retired, drop once verified)',
        p_partition, clock_timestamp() - v_started, v_values;
END;
$$;
-- +goose StatementEnd

-- +goose Down
DROP PROCEDURE IF EXISTS pred.rebuild_forecast_partition(TEXT, INTERVAL, TEXT);

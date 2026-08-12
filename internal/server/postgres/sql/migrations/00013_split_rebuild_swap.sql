-- +goose Up

-- +goose StatementBegin
/*
 * Splits pred.rebuild_forecast_partition into a build phase and a swap phase.
 *
 * 00012 did both in one transaction, and deadlocked against the live read path:
 *
 *     the reader held AccessShare on pred.forecasters - the read queries resolve the forecaster
 *     id before they touch pred.forecasts - and waited for AccessShare on pred.forecasts;
 *     the rebuild held AccessExclusive on pred.forecasts from its DETACH, and waited for
 *     AccessExclusive on pred.forecasters, which DROP TABLE needs in order to remove the
 *     partition's foreign key triggers from the referenced side.
 *
 * DROP TABLE was the only statement in the swap that reached beyond pred.forecasts and its
 * values sibling: pred.forecasts references pred.forecasters, loc.geometries and
 * loc.source_types, and dropping a referencing table takes AccessExclusive on all three. Every
 * other step touches those tables at ShareRowExclusive at most (ATTACH clones the parent's
 * foreign keys onto the new partition), which readers never block. So the old partition is now
 * renamed to _retired rather than dropped - matching how the values partition is already
 * treated - and dropped out of band once the rebuild has been verified.
 *
 * The swap also takes the locks it needs up front, pred.forecasts before
 * pred.predicted_generation_values, which is the order every reader and writer takes them in.
 * Holding nothing else at that point, a conflict is a lock_timeout on the first statement rather
 * than a cycle discovered twelve minutes in.
 *
 * Splitting the phases is what makes losing that race cheap: the staging fill and the ordered
 * insert are committed before the swap begins, so a swap can be retried on its own.
 *
 *     CALL pred.build_forecast_partition('forecasts_p20260803');
 *     CALL pred.swap_forecast_partition('forecasts_p20260803');
 */
CREATE OR REPLACE PROCEDURE pred.build_forecast_partition(
    p_partition TEXT,
    p_chunk INTERVAL DEFAULT INTERVAL '1 hour',
    p_work_mem TEXT DEFAULT '256MB'
)
LANGUAGE plpgsql AS $$
DECLARE
    v_values  TEXT;
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

    /* A rebuilt table left by a run whose swap failed holds verified rows: swapping it in is
     * cheaper and safer than rebuilding it, so refuse rather than silently starting over. */
    IF to_regclass('pred.' || quote_ident(v_new)) IS NOT NULL THEN
        RAISE EXCEPTION 'pred.% already exists',
            v_new
        USING HINT = format(
            'CALL pred.swap_forecast_partition(%L) to finish that run, or DROP TABLE pred.%I to start over',
            p_partition, v_new);
    END IF;

    v_lo := partman.uuid7_time_decoder(
        (regexp_match(v_bounds, $re$FROM \('([^']+)'\)$re$))[1]::UUID::TEXT) AT TIME ZONE 'UTC';
    v_hi := partman.uuid7_time_decoder(
        (regexp_match(v_bounds, $re$TO \('([^']+)'\)$re$))[1]::UUID::TEXT) AT TIME ZONE 'UTC';

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

    EXECUTE format('SET LOCAL work_mem = %L', p_work_mem);

    /* Ordering by (geometry_uuid, source_type_id, forecaster_id, forecast_uuid DESC) makes one
     * location's forecasts contiguous and matches idx_forecasts_filter, so an index scan walks
     * the heap in physical order. Every hot-path query filters on geometry_uuid first, so
     * nothing loses. StreamForecastData is the only broad scan and is explicitly rare.
     *
     * Forecasts with no rows in the values partition are dropped: the INNER JOIN against staging
     * excludes them, and the row count check below is written to expect that. */
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

    /* Marks the partition as migrated. Note this does not buy constraint exclusion on the read
     * queries' legacy branch: they filter p50_sips inside a CTE rather than on a direct scan of
     * pred.forecasts, so the planner cannot use it to prune. It is an integrity check and an
     * operational marker for which partitions are done. Added before ATTACH so the fresh,
     * exclusively-locked table is scanned rather than a live partition. */
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
 *
 * Metadata only, so this is seconds rather than minutes - the one exception is ATTACH, which
 * validates the partition bound and clones the parent's foreign keys onto the new partition.
 *
 * pred.predicted_generation_values carries a foreign key to pred.forecasts, and PostgreSQL
 * refuses to detach a partition that is still referenced, so the matching values partition is
 * detached first. Detaching leaves a standalone copy of the foreign key behind on it, which
 * would still block the forecasts detach, so that constraint is dropped too - it takes
 * AccessExclusive on pred.forecasts, which is already held.
 *
 * Both old tables are renamed rather than dropped. Dropping them takes AccessExclusive on every
 * table they reference (pred.forecasters, loc.geometries, loc.source_types), which is what
 * deadlocked against the read path in 00012, and it is the point of no return for the week.
 * Drop them out of band once the rebuild is verified.
 *
 *     CALL pred.swap_forecast_partition('forecasts_p20260803')
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

    v_values := 'predicted_generation_values_' || substring(p_partition FROM '^forecasts_(p.+)$');

    IF v_values IS NULL OR to_regclass('pred.' || quote_ident(v_values)) IS NULL THEN
        RAISE EXCEPTION 'no values partition matching pred.%: expected pred.%',
            p_partition, v_values;
    END IF;

    /* Everything below needs these two, so take them now, while nothing else is held, in the
     * order the read and write paths take them: forecasts before predicted_generation_values.
     * A conflict then fails here, before any DDL has run, and the call can simply be repeated.
     * Both are partitioned parents and LOCK recurses, so this covers the partitions too. */
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

    /* Renaming a table does not rename its indexes, so the swapped-in partition would keep the
     * _v2 names it was built under - and the retired table would keep the names it should have.
     * Move the retired ones aside, then name each new index after the one it replaces. Matching
     * is on the index definition rather than the name because PostgreSQL truncates names to 63
     * bytes, which bites at a different point for the longer _v2 table name. Renaming an index
     * renames the constraint behind it, so primary keys are covered by the same loop. */
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
        /* An index with no counterpart on the retired table - the parent's index set has moved
         * on since that partition was made - just loses the _v2 from the name it was built with. */
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

-- +goose Up

/*
 * Consolidates the array storage migration (00011-00013).
 *
 * Makes p50_sips NOT NULL, drops pred.predicted_generation_values and the machinery that
 * rebuilt partitions into it, and leaves pred.forecasts as the single source of predicted
 * values.
 *
 * This is not reversible. The Down below restores the schema so that a code revert works,
 * but the row data is gone.
 */

-- +goose StatementBegin
DO $$
DECLARE
    v_unmigrated BIGINT;
BEGIN
    SELECT count(*) INTO v_unmigrated FROM pred.forecasts WHERE p50_sips IS NULL;

    IF v_unmigrated > 0 THEN
        RAISE EXCEPTION 'refusing to drop row storage: % forecasts still have no arrays',
            v_unmigrated
        USING HINT = 'find them with: SELECT tableoid::REGCLASS, count(*) FROM pred.forecasts '
                     'WHERE p50_sips IS NULL GROUP BY 1';
    END IF;
END $$;
-- +goose StatementEnd

/* Every partition rebuilt by 00013 already carries a validated
 * CHECK (p50_sips IS NOT NULL) named p50_sips_not_null, which lets SET NOT NULL skip its
 * scan. Partitions pg_partman created after the array deploy carry no such constraint -
 * they are array-native by construction but unproven, so prove them here. Adding NOT VALID
 * and validating separately keeps the scan under SHARE UPDATE EXCLUSIVE instead of
 * ACCESS EXCLUSIVE. */
-- +goose StatementBegin
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT c.relname
        FROM pg_class AS c
            INNER JOIN pg_inherits AS i ON i.inhrelid = c.oid
            INNER JOIN pg_class AS p ON p.oid = i.inhparent
        WHERE p.relname = 'forecasts'
            AND NOT EXISTS (
                SELECT 1 FROM pg_constraint AS k
                WHERE k.conrelid = c.oid
                    AND k.conname = 'p50_sips_not_null'
                    AND k.convalidated
            )
    LOOP
        RAISE NOTICE 'proving p50_sips on pred.%', r.relname;
        EXECUTE format(
            'ALTER TABLE pred.%I ADD CONSTRAINT p50_sips_not_null '
            'CHECK (p50_sips IS NOT NULL) NOT VALID', r.relname);
        EXECUTE format(
            'ALTER TABLE pred.%I VALIDATE CONSTRAINT p50_sips_not_null', r.relname);
    END LOOP;
END $$;
-- +goose StatementEnd

ALTER TABLE pred.forecasts ALTER COLUMN p50_sips SET NOT NULL;

/* The per-partition CHECKs were the marker for which partitions had been rebuilt, and the
 * proof that let SET NOT NULL skip its scan. The column constraint now subsumes both. */
-- +goose StatementBegin
DO $$
DECLARE
    r RECORD;
BEGIN
    FOR r IN
        SELECT c.relname
        FROM pg_class AS c
            INNER JOIN pg_inherits AS i ON i.inhrelid = c.oid
            INNER JOIN pg_class AS p ON p.oid = i.inhparent
            INNER JOIN pg_constraint AS k
                ON k.conrelid = c.oid AND k.conname = 'p50_sips_not_null'
        WHERE p.relname = 'forecasts'
    LOOP
        EXECUTE format(
            'ALTER TABLE pred.%I DROP CONSTRAINT p50_sips_not_null', r.relname);
    END LOOP;
END $$;
-- +goose StatementEnd

/* p50_sips can no longer be NULL, so the guard is dead. Left NOT VALID, as it has been
 * since 00011 - validating it is a full scan and buys nothing the write path does not
 * already enforce. */
ALTER TABLE pred.forecasts DROP CONSTRAINT plevel_lengths_match_check;

ALTER TABLE pred.forecasts
    ADD CONSTRAINT plevel_lengths_match_check CHECK (
        ARRAY_LENGTH(p50_sips, 1) > 0
        AND COALESCE(ARRAY_LENGTH(p02_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        AND COALESCE(ARRAY_LENGTH(p10_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        AND COALESCE(ARRAY_LENGTH(p25_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        AND COALESCE(ARRAY_LENGTH(p75_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        AND COALESCE(ARRAY_LENGTH(p90_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        AND COALESCE(ARRAY_LENGTH(p98_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
    ) NOT VALID;

/* pg_partman config goes before the table, or the next run_maintenance errors on a parent
 * that no longer exists. */
DELETE FROM partman.part_config_sub
WHERE sub_parent = 'pred.predicted_generation_values';

DELETE FROM partman.part_config
WHERE parent_table = 'pred.predicted_generation_values';

/* Takes ACCESS EXCLUSIVE on pred.forecasters, loc.geometries and loc.source_types to remove
 * this table's foreign key triggers from the referenced side - a lock set that can deadlock
 * against the read path. Safe only because goose runs with the API down. */
SET lock_timeout = '30s';

DROP TABLE pred.predicted_generation_values;
DROP TABLE IF EXISTS pred.predicted_generation_values_template;
DROP TABLE IF EXISTS partman.template_pred_predicted_generation_values;

RESET lock_timeout;

/* Only reference was the other_stats_fractions constraint on the table just dropped. */
DROP FUNCTION IF EXISTS pred.check_all_jsonb_values_are_valid_stat_fractions(JSONB);

DROP PROCEDURE IF EXISTS pred.swap_forecast_partition(TEXT, TEXT);
DROP PROCEDURE IF EXISTS pred.build_forecast_partition(TEXT, INTERVAL, TEXT);
DROP TABLE IF EXISTS pred.fc_staging;

-- +goose Down

/*
 * Restores the schema, not the data. The row storage this migration dropped is gone; the
 * arrays on pred.forecasts remain authoritative. This exists so that reverting the binary
 * leaves a database whose shape the old code recognises - the legacy read branches will
 * simply find no rows, which is the correct answer now that every forecast has arrays.
 */

ALTER TABLE pred.forecasts ALTER COLUMN p50_sips DROP NOT NULL;

ALTER TABLE pred.forecasts DROP CONSTRAINT plevel_lengths_match_check;

ALTER TABLE pred.forecasts
    ADD CONSTRAINT plevel_lengths_match_check CHECK (
        p50_sips IS NULL OR (
            ARRAY_LENGTH(p50_sips, 1) > 0
            AND COALESCE(ARRAY_LENGTH(p02_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
            AND COALESCE(ARRAY_LENGTH(p10_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
            AND COALESCE(ARRAY_LENGTH(p25_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
            AND COALESCE(ARRAY_LENGTH(p75_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
            AND COALESCE(ARRAY_LENGTH(p90_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
            AND COALESCE(ARRAY_LENGTH(p98_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        )
    ) NOT VALID;

CREATE TABLE pred.predicted_generation_values (
    horizon_mins SMALLINT NOT NULL,
    CONSTRAINT horizon_mins_nonnegative_check CHECK (horizon_mins >= 0),
    CONSTRAINT horizon_mins_fiveminutely_check CHECK (horizon_mins % 5 = 0),
    p50_sip SMALLINT NOT NULL,
    CONSTRAINT p50_sip_nonnegative_check CHECK (p50_sip >= 0),
    p10_sip SMALLINT,
    CONSTRAINT p10_sip_nonnegative_check CHECK (p10_sip >= 0),
    p90_sip SMALLINT,
    CONSTRAINT p90_sip_nonnegative_check CHECK (p90_sip >= 0),
    p02_sip SMALLINT,
    CONSTRAINT p02_sip_nonnegative_check CHECK (p02_sip >= 0),
    p25_sip SMALLINT,
    CONSTRAINT p25_sip_nonnegative_check CHECK (p25_sip >= 0),
    p75_sip SMALLINT,
    CONSTRAINT p75_sip_nonnegative_check CHECK (p75_sip >= 0),
    p98_sip SMALLINT,
    CONSTRAINT p98_sip_nonnegative_check CHECK (p98_sip >= 0),
    forecast_uuid UUID NOT NULL
    REFERENCES pred.forecasts (forecast_uuid)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
    PRIMARY KEY (forecast_uuid, horizon_mins)
)
PARTITION BY RANGE (forecast_uuid);

SELECT partman.create_parent(
    p_parent_table => 'pred.predicted_generation_values',
    p_control => 'forecast_uuid',
    p_type => 'range',
    p_interval => '1 week',
    p_automatic_maintenance => 'on',
    p_jobmon => FALSE,
    p_time_encoder => 'partman.uuid7_time_encoder',
    p_time_decoder => 'partman.uuid7_time_decoder',
    p_premake => 7
);

UPDATE partman.part_config
SET
    retention_keep_table = TRUE,
    retention_keep_index = TRUE,
    infinite_time_partitions = TRUE
WHERE parent_table = 'pred.predicted_generation_values';

SELECT partman.run_maintenance('pred.predicted_generation_values');

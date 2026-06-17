-- +goose NO TRANSACTION
-- +goose Up

/*
 * Reduces the database size by approximately 40%.
 *
 * Modifies the predicted_generation_values table to optimize it's use of storage.
 * This is done through changing the index, removing redundant columns, and replacing
 * dynamic columns with small static ones.
 *
 * The schema modifications and the corresponding data changes are seperated out for
 * faster migration. Since the predicted_generation_values table is very large, simple
 * DELETES and UPDATES would take a long time, and not actually gain us any storage
 * savings (at least until an autovacuum process ran). By instead moving the data
 * partition-wise and then replacing the partitions, we keep the process light on CPU.
 */ 

DROP INDEX IF EXISTS loc.idx_sources_mv_gist_sys_period;

CREATE INDEX IF NOT EXISTS idx_sources_mv_composite_lookup 
ON loc.sources_mv USING gist (geometry_uuid, source_type_id, sys_period);

-- +goose StatementBegin
DO $$
DECLARE
    pk_name TEXT;
BEGIN
    SELECT conname INTO pk_name
    FROM pg_constraint
    WHERE conrelid = 'pred.predicted_generation_values'::regclass
      AND contype = 'p';

    IF pk_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE pred.predicted_generation_values DROP CONSTRAINT %I CASCADE;', pk_name);
    END IF;
END $$;
-- +goose StatementEnd

ALTER TABLE pred.predicted_generation_values 
    ADD COLUMN IF NOT EXISTS p10_sip SMALLINT, 
    ADD COLUMN IF NOT EXISTS p90_sip SMALLINT,
    ALTER COLUMN target_time_utc DROP NOT NULL;

-- +goose StatementBegin
CREATE OR REPLACE PROCEDURE pred.swap_predicted_generation_partitions()
LANGUAGE plpgsql
AS $$
DECLARE
    partition_record RECORD;
    new_part_name TEXT;
    part_bound TEXT;
    is_migrated BOOLEAN;
BEGIN
    FOR partition_record IN
        SELECT child.relname AS table_name,
               pg_get_expr(child.relpartbound, child.oid) AS bounds
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
        WHERE parent.relname = 'predicted_generation_values'
          AND nmsp_parent.nspname = 'pred'
    LOOP
	EXECUTE format('
            SELECT EXISTS (
                SELECT 1
                FROM pg_index
                JOIN pg_class ON pg_index.indrelid = pg_class.oid
                JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid
                WHERE pg_namespace.nspname = ''pred''
                  AND pg_class.relname = %L
                  AND pg_index.indisprimary
            )', partition_record.table_name)
        INTO is_migrated;

	IF is_migrated THEN
            RAISE NOTICE 'Skipping partition (already migrated): %', partition_record.table_name;
            CONTINUE;
        END IF;
	
	RAISE NOTICE 'Migrating partition: %', partition_record.table_name;

        new_part_name := partition_record.table_name || '_v2';
        part_bound := partition_record.bounds;

        EXECUTE format('CREATE TABLE pred.%I (LIKE pred.predicted_generation_values INCLUDING ALL);', new_part_name);

        EXECUTE format('
            INSERT INTO pred.%I (horizon_mins, p50_sip, p10_sip, p90_sip, forecast_uuid, target_time_utc, metadata, other_stats_fractions)
            SELECT 
                horizon_mins, 
                p50_sip, 
                CASE 
                    WHEN other_stats_fractions IS NULL THEN p10_sip
                    ELSE LEAST(((other_stats_fractions->>''p10'')::REAL * 30000), 32767)::SMALLINT
                END, 
                CASE 
                    WHEN other_stats_fractions IS NULL THEN p90_sip
                    ELSE LEAST(((other_stats_fractions->>''p90'')::REAL * 30000), 32767)::SMALLINT
                END, 
                forecast_uuid, 
                NULL, 
                NULL, 
                NULL 
            FROM pred.%I;
        ', new_part_name, partition_record.table_name);

        EXECUTE format('ALTER TABLE pred.%I ADD PRIMARY KEY (forecast_uuid, horizon_mins);', new_part_name);
        EXECUTE format('ALTER TABLE pred.predicted_generation_values DETACH PARTITION pred.%I;', partition_record.table_name);
        EXECUTE format('ALTER TABLE pred.predicted_generation_values ATTACH PARTITION pred.%I %s;', new_part_name, part_bound);
        EXECUTE format('DROP TABLE pred.%I;', partition_record.table_name);
        EXECUTE format('ALTER TABLE pred.%I RENAME TO %I;', new_part_name, partition_record.table_name);

        COMMIT; 
    END LOOP;
END;
$$;
-- +goose StatementEnd

CALL pred.swap_predicted_generation_partitions();
DROP PROCEDURE pred.swap_predicted_generation_partitions;

ALTER TABLE pred.predicted_generation_values
    DROP COLUMN target_time_utc,
    DROP COLUMN other_stats_fractions,
    DROP COLUMN metadata,
    ADD PRIMARY KEY (forecast_uuid, horizon_mins);

DROP TABLE IF EXISTS pred.predicted_generation_values_template;
CREATE TABLE pred.predicted_generation_values_template (
    horizon_mins SMALLINT NOT NULL,
    CONSTRAINT horizon_mins_nonnegative_check CHECK (horizon_mins >= 0),
    CONSTRAINT horizon_mins_fiveminutely_check CHECK (horizon_mins % 5 = 0),
    p50_sip SMALLINT NOT NULL,
    CONSTRAINT p50_sip_nonnegative_check CHECK (p50_sip >= 0),
    p10_sip SMALLINT,
    p90_sip SMALLINT,
    forecast_uuid UUID NOT NULL REFERENCES pred.forecasts (forecast_uuid) ON DELETE CASCADE ON UPDATE CASCADE, 
    PRIMARY KEY (forecast_uuid, horizon_mins)
);

ANALYZE pred.predicted_generation_values;


-- +goose Down
ALTER TABLE pred.predicted_generation_values
    DROP CONSTRAINT predicted_generation_values_pkey CASCADE,
    ADD COLUMN target_time_utc TIMESTAMP,
    ADD COLUMN other_stats_fractions JSONB DEFAULT NULL,
    ADD CONSTRAINT other_stats_nullifempty CHECK (other_stats_fractions IS NULL OR other_stats_fractions != '{}'),
    ADD CONSTRAINT other_stats_valid_fractions_check CHECK (pred.check_all_jsonb_values_are_valid_stat_fractions(other_stats_fractions)),
    ADD COLUMN metadata JSONB DEFAULT NULL;


-- +goose StatementBegin
CREATE OR REPLACE PROCEDURE pred.rollback_predicted_generation_partitions()
LANGUAGE plpgsql
AS $$
DECLARE
    partition_record RECORD;
BEGIN
    FOR partition_record IN
        SELECT child.relname AS table_name
        FROM pg_inherits
        JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
        JOIN pg_class child ON pg_inherits.inhrelid = child.oid
        JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
        WHERE parent.relname = 'predicted_generation_values'
          AND nmsp_parent.nspname = 'pred'
    LOOP
        EXECUTE format('
            UPDATE pred.%I
            SET target_time_utc = UUIDV7_EXTRACT_TIMESTAMP(forecast_uuid)::TIMESTAMP + MAKE_INTERVAL(mins => horizon_mins::INTEGER),
	    other_stats_fractions = CASE WHEN p10_sip IS NOT NULL OR p90_sip IS NOT NULL THEN jsonb_strip_nulls(jsonb_build_object(''p10'', p10_sip::REAL / 30000, ''p90'', p90_sip::REAL / 30000))
            ELSE NULL END;
        ', partition_record.table_name);

        EXECUTE format('ALTER TABLE pred.%I ALTER COLUMN target_time_utc SET NOT NULL;', partition_record.table_name);
        EXECUTE format('ALTER TABLE pred.%I ADD PRIMARY KEY (forecast_uuid, target_time_utc);', partition_record.table_name);
        
        COMMIT;
    END LOOP;
END;
$$;
-- +goose StatementEnd

CALL pred.rollback_predicted_generation_partitions();
DROP PROCEDURE pred.rollback_predicted_generation_partitions;

ALTER TABLE pred.predicted_generation_values
    ALTER COLUMN target_time_utc SET NOT NULL,
    ADD PRIMARY KEY (forecast_uuid, target_time_utc),
    DROP COLUMN p10_sip, DROP COLUMN p90_sip;

DROP TABLE IF EXISTS pred.predicted_generation_values_template;
CREATE TABLE pred.predicted_generation_values_template (
    horizon_mins SMALLINT NOT NULL,
    CONSTRAINT horizon_mins_nonnegative_check CHECK (horizon_mins >= 0),
    CONSTRAINT horizon_mins_fiveminutely_check CHECK (horizon_mins % 5 = 0),
    p50_sip SMALLINT NOT NULL,
    CONSTRAINT p50_sip_nonnegative_check CHECK (p50_sip >= 0),
    target_time_utc TIMESTAMP NOT NULL,
    forecast_uuid UUID NOT NULL REFERENCES pred.forecasts (forecast_uuid) ON DELETE CASCADE ON UPDATE CASCADE,
    metadata JSONB DEFAULT NULL CONSTRAINT metadata_nullifempty CHECK (metadata IS NULL OR metadata != '{}'),
    other_stats_fractions JSONB DEFAULT NULL CONSTRAINT other_stats_nullifempty CHECK (other_stats_fractions IS NULL OR other_stats_fractions != '{}'),
    CONSTRAINT other_stats_valid_fractions_check CHECK (pred.check_all_jsonb_values_are_valid_stat_fractions(other_stats_fractions)),
    PRIMARY KEY (forecast_uuid, target_time_utc)
);

ANALYZE pred.predicted_generation_values;

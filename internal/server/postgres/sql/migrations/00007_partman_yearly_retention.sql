-- +goose Up

/*
 * Removes the month-long retention policy on partman-managed partitions.
 *
 * This allows for querying of all historical data without needing custom queries. However, some
 * partitions may have already been detached by the previous retention policy, so they must also
 * be re-attached to the parent table. This is done by iterating through all existing partitions,
 * extracting the date from their name, and attaching them with the appropriate range values.
 */

DO $$
DECLARE
    partition_record RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    attach_sql TEXT;
BEGIN
    UPDATE partman.part_config
    SET retention = NULL
    WHERE parent_table = 'obs.observed_generation_values';

    FOR partition_record IN 
        SELECT 
            table_schema || '.' || table_name AS full_table_name,
            SUBSTRING(table_name FROM 'p(\d{8})$') AS date_str
        FROM information_schema.tables
        WHERE table_schema = 'obs'
          AND table_name LIKE 'observed_generation_values_p________'
          AND NOT EXISTS (
              SELECT 1 FROM pg_inherits
              WHERE inhrelid = (table_schema || '.' || table_name)::regclass
                AND inhparent = 'obs.observed_generation_values'::regclass
          )
    LOOP
        start_time := to_timestamp(partition_record.date_str, 'YYYYMMDD');
        end_time := start_time + INTERVAL '1 week';
        attach_sql := format(
            'ALTER TABLE obs.observed_generation_values ATTACH PARTITION %s FOR VALUES FROM (%L) TO (%L);',
            partition_record.full_table_name,
            start_time,
            end_time
        );
        RAISE NOTICE 'Executing: %', attach_sql;
        EXECUTE attach_sql;
    END LOOP;
END $$;

DO $$
DECLARE
    target_table TEXT;
    parent_table_name TEXT;
    partition_pattern TEXT;
    partition_record RECORD;
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    attach_sql TEXT;
BEGIN
    FOREACH target_table IN ARRAY ARRAY['forecasts', 'predicted_generation_values']
    LOOP
	parent_table_name := 'pred.' || target_table;
	partition_pattern := target_table || '_p________';

        UPDATE partman.part_config
    	SET retention = NULL
   	WHERE parent_table = parent_table_name;

	FOR partition_record IN 
            SELECT 
                table_schema || '.' || table_name AS full_table_name,
                SUBSTRING(table_name FROM 'p(\d{8})$') AS date_str
            FROM information_schema.tables
            WHERE table_schema = 'pred'
              AND table_name LIKE partition_pattern
              AND NOT EXISTS (
                  SELECT 1 FROM pg_inherits
                  WHERE inhrelid = (table_schema || '.' || table_name)::regclass
                    AND inhparent = parent_table_name::regclass
              )
        LOOP
            start_time := to_timestamp(partition_record.date_str, 'YYYYMMDD');
            end_time := start_time + INTERVAL '1 week';
            
            attach_sql := format(
                'ALTER TABLE %s ATTACH PARTITION %s FOR VALUES FROM (UUIDV7_BOUNDARY(%L::TIMESTAMP)) TO (UUIDV7_BOUNDARY(%L::TIMESTAMP));',
                parent_table_name,
                partition_record.full_table_name,
                start_time,
                end_time
            );
            
            RAISE NOTICE 'Executing: %', attach_sql;
            EXECUTE attach_sql;
        END LOOP;
    END LOOP;
END $$;

-- 

-- +goose Up

CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS postgis WITH SCHEMA public;
CREATE SCHEMA IF NOT EXISTS partman;
CREATE EXTENSION IF NOT EXISTS pg_partman WITH SCHEMA partman;
CREATE EXTENSION IF NOT EXISTS pg_cron;

SELECT cron.schedule('cron-details-cleanup', '0 12 * * *', $$DELETE FROM cron.job_run_details WHERE end_time < now() - interval '7 days'$$);


/* Overwrites the default uuidv7_extract_timestamp function.
 * The default function uses the system local timezone to return a TIMESTAMPTZ.
 * In order to make the function SAFE, this dependency is removed.
 * NOTE: Requires the local timezone to be UTC.
 */
-- +goose StatementBegin
CREATE FUNCTION uuidv7_extract_timestamp(u UUID) RETURNS TIMESTAMP
   LANGUAGE sql
   IMMUTABLE STRICT PARALLEL SAFE
   RETURN uuid_extract_timestamp(u) AT TIME ZONE 'UTC';
-- +goose StatementEnd

/* Generate a non-random uuidv7 with the given timestamp (first 48 bits) and all random bits to 0.
 * As the smallest possible uuidv7 for that timestamp, it may be used as a boundary for partitions.
 */
-- +goose StatementBegin
CREATE FUNCTION uuidv7_boundary(timestamptz) RETURNS uuid
AS $$
  /* uuid fields: version=0b0111, variant=0b10 */
  select encode(
    overlay('\x00000000000070008000000000000000'::bytea
      placing substring(int8send(floor(extract(epoch from $1) * 1000)::bigint) from 3)
        from 1 for 6),
    'hex')::uuid;
$$ LANGUAGE sql stable strict parallel safe;
-- +goose StatementEnd


/* == LOCATIONS ===================================================================================
 *
 * Schema and tables to handle location data.
 *
 * The generation data we store, be it predicted or otherwise, is always tied to a certain
 * geometry. These geometries vary in size and scope, from a single site to an entire country,
 * and the metadata we may want to store about them will also vary accordingly.

 * From an application standpoint, the geometry is pertinent in the case where we care about the
 * generated power as a fraction of the capacity of the geometry, as well as allowing us to
 * represent the data on a map.

 * To this degree, what the external application may consider a "location", is represented here as
 * a combination of a geometry (the spatial data), and a source (the energy generation capability).
 * One geometry can have multiple sources, e.g. the UK nation geometry can have solar, wind, etc.
 */

CREATE SCHEMA loc;

/*- Lookups -----------------------------------------------------------------------------------*/

-- Lookup table to store different source types
CREATE TABLE loc.source_types (
    source_type_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    source_type_name TEXT NOT NULL,
    CONSTRAINT source_type_name_format_check CHECK (
        LENGTH(source_type_name) > 0
        AND LENGTH(source_type_name) <= 48
        AND source_type_name = LOWER(source_type_name)
    ),
    PRIMARY KEY (source_type_id),
    UNIQUE (source_type_name)
);
-- The ordering of insertion here matches the .proto enum definitions. Change with caution!
INSERT INTO loc.source_types (source_type_name) VALUES ('solar'), ('wind'), ('hydro'), ('battery');

-- Lookup table to store different geometry types
CREATE TABLE loc.geometry_types (
    geometry_type_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    geometry_type_name TEXT NOT NULL,
    CONSTRAINT geometry_type_name_format_check CHECK (
        LENGTH(geometry_type_name) > 0
        AND LENGTH(geometry_type_name) <= 24
        AND geometry_type_name = LOWER(geometry_type_name)
    ),
    PRIMARY KEY (geometry_type_id),
    UNIQUE (geometry_type_name)
);
-- The ordering of insertion here matches the .proto enum definitions. Change with caution!
INSERT INTO loc.geometry_types (geometry_type_name) VALUES ('site'), ('gsp'), ('dno'), ('nation'), ('state'), ('county'), ('city'), ('primary_substation');


/*- Tables ----------------------------------------------------------------------------------*/

CREATE TABLE loc.entities (
    entity_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    external_id TEXT NOT NULL
    CONSTRAINT external_id_format_check CHECK (
	external_id IS NOT NULL
	AND LENGTH(external_id) > 0
	AND LENGTH(external_id) <= 128
    ),
    UNIQUE (external_id)
);


-- Table to store spatial data for geometries
CREATE TABLE loc.geometries (
    geometry_uuid UUID DEFAULT UUIDV7() NOT NULL,
    geometry_name TEXT NOT NULL,
    CONSTRAINT geometry_name_check CHECK (
        LENGTH(geometry_name) > 0
        AND geometry_name = LOWER(geometry_name)
    ),
    geom GEOMETRY (GEOMETRY, 4326) NOT NULL,
    CONSTRAINT geom_validity_check CHECK (
        ST_GEOMETRYTYPE(geom) IN ('ST_Point', 'ST_Polygon', 'ST_MultiPolygon')
        AND ST_SRID(geom) = 4326
        AND ST_NDIMS(geom) = 2
        AND ST_ISVALID(geom)
        AND ST_XMIN(geom) >= -180 AND ST_XMAX(geom) <= 180
        AND ST_YMIN(geom) >= -90 AND ST_YMAX(geom) <= 90
    ),
    geometry_type_id SMALLINT NOT NULL
    REFERENCES loc.geometry_types (geometry_type_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
    associated_point GEOMETRY (POINT, 4326) NOT NULL,
    CONSTRAINT associated_point_validity_check CHECK (
	ST_SRID(associated_point) = 4326
	AND ST_NDIMS(associated_point) = 2
	AND ST_ISVALID(associated_point)
	AND ST_X(associated_point) >= -180 AND ST_X(associated_point) <= 180
	AND ST_Y(associated_point) >= -90 AND ST_Y(associated_point) <= 90
    ),
    geom_hash TEXT GENERATED ALWAYS AS (MD5(ST_ASBINARY(geom))) STORED,
    metadata JSONB DEFAULT NULL,
    owning_entity_id INTEGER DEFAULT NULL
    REFERENCES loc.entities(entity_id)
    ON UPDATE CASCADE
    ON DELETE SET NULL,
    PRIMARY KEY (geometry_uuid),
    UNIQUE (geometry_name, geom_hash)
);
-- Required index for efficient spatial-based queries
CREATE INDEX ON loc.geometries USING gist (geom);
-- Index for efficiently fetching e.g. all POINT geometry geometries
CREATE INDEX ON loc.geometries (ST_GEOMETRYTYPE(geom));
-- Index for finding all geometries of a certain type
CREATE INDEX ON loc.geometries (geometry_type_id);
-- Legacy index for finding gsp geometries by gsp_id
CREATE INDEX idx_geometries_gsp_id_partial
ON loc.geometries (((metadata ->> 'gsp_id')::INTEGER))
WHERE geometry_type_id = 2
  AND (((metadata ->> 'gsp_id') IS NOT NULL));
-- Index for finding all geometries owned by a certain entity
CREATE INDEX idx_owning_entity_id ON loc.geometries (owning_entity_id);

/*
 * Table to store the temporal generation capability of geometries.
 * Each geometry can have multiple sources of generation (solar, wind, etc),
 * and each source can change over time. For speed of writing, this is handled
 * via a simple valid-from timestamp field.
 */
CREATE TABLE loc.sources_history (
    source_type_id SMALLINT NOT NULL
    REFERENCES loc.source_types (source_type_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
    -- Capacity cap, (for instance during curtailment or repair work),
    -- encoded as a smallint percentage (sip) of the capacity; with 0 representing 0%
    -- AND 30000 representing 100% of the capacity. However, since things are mostly
    -- not limited, NULL indicates no limit, so 30000 is an invalid value.
    -- NOTE: This is currently not used.
    capacity_limit_sip SMALLINT DEFAULT NULL,
    CONSTRAINT capacity_limit_sip_vaildity_check CHECK (
        capacity_limit_sip IS NULL
        OR (capacity_limit_sip >= 0 AND capacity_limit_sip < 30000)
    ),
    -- Capacity in watts. This maxes out at ~9.22 petawatts, which should be sufficient
    capacity_watts BIGINT NOT NULL,
    CONSTRAINT capacity_nonnegative_check CHECK (capacity_watts >= 0),
    valid_from_utc TIMESTAMP DEFAULT NOW() NOT NULL,
    geometry_uuid UUID NOT NULL
    REFERENCES loc.geometries (geometry_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    -- Metadata about the source, e.g. tilt, orientation, etc.
    metadata JSONB DEFAULT NULL,
    CONSTRAINT metadata_nonempty_check CHECK (
        metadata IS NULL OR metadata <> '{}'::JSONB -- Null is cheaper
    ),
    PRIMARY KEY (geometry_uuid, source_type_id, valid_from_utc)
);

/*
 * Materialized view to store the state of sources over time with a system period.
 * This allows for quicker reads of the state of sources at a given time.
 */
CREATE MATERIALIZED VIEW loc.sources_mv AS
SELECT
    sh.geometry_uuid,
    sh.source_type_id,
    sh.capacity_watts,
    sh.capacity_limit_sip,
    sh.metadata,
    COALESCE(sh.metadata || g.metadata, sh.metadata, g.metadata)::JSONB AS metadata_jsonb,
    g.geometry_name,
    g.geometry_type_id,
    g.owning_entity_id,
    ST_X(g.associated_point)::REAL AS longitude,
    ST_Y(g.associated_point)::REAL AS latitude,
    TSRANGE(
        sh.valid_from_utc,
        LEAD(sh.valid_from_utc, 1) OVER (
            PARTITION BY sh.geometry_uuid, sh.source_type_id
            ORDER BY sh.valid_from_utc
        )
    ) AS sys_period
FROM loc.sources_history AS sh
INNER JOIN loc.geometries AS g USING (geometry_uuid);
-- Prevent overlapping records. Required for concurrent refreshes.
CREATE UNIQUE INDEX ON loc.sources_mv (geometry_uuid, source_type_id, sys_period);
CREATE INDEX idx_sources_mv_owning_entity_id ON loc.sources_mv (owning_entity_id);
CREATE INDEX idx_sources_mv_composite_lookup ON loc.sources_mv USING gist (geometry_uuid, source_type_id, sys_period);


/* == OBSERVATIONS ================================================================================
 *
 * Schema and tables to handle observed generation data.
 *
 * Observations of generation data is usually measured by providers of inverters, which are
 * required in many sources of renewable energy to convert power from DC to AC. Partnerships
 * with these providers provide access to the data in order to test the accuracy of predictions.
*/


CREATE SCHEMA obs;

/*- Tables ----------------------------------------------------------------------------------*/

/*
 * Table to store observers.
 * These are providers of actual recorded generation values from inverters
 * (mostly - looking at you, pvlive...)
*/
CREATE TABLE obs.observers (
    observer_uuid UUID NOT NULL DEFAULT UUIDV7(),
    observer_name TEXT NOT NULL,
    CONSTRAINT observer_name_format_check CHECK (
        LENGTH(observer_name) > 0 AND LENGTH(observer_name) < 128
        AND observer_name = LOWER(observer_name)
    ),
    PRIMARY KEY (observer_uuid),
    UNIQUE (observer_name)
);

/*
 * Table to store observed generation values.
 * The generation value is stored as a percentage of the source capacity represented by a
 * smallint percent (sip). Since it isn't impossible to measure a little over capacity, 30000
 * represents 100% of capacity instead of the max smallint value (32767). This allows for some
 * measurement leeway.
 * The table has native partitioning that can then be managed by pg_partman. Note that unique
 * indexes will only work if they include the partition key.
 */
CREATE TABLE obs.observed_generation_values (
    value_sip SMALLINT NOT NULL,
    CONSTRAINT value_sip_nonnegative_check CHECK (value_sip >= 0),
    source_type_id SMALLINT NOT NULL
    REFERENCES loc.source_types (source_type_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
    observation_timestamp_utc TIMESTAMP NOT NULL,
    CONSTRAINT observation_timestamp_utc_recency_check CHECK (
        observation_timestamp_utc <= CURRENT_TIMESTAMP + MAKE_INTERVAL(days => 31)
    ),
    observer_uuid UUID NOT NULL
    REFERENCES obs.observers (observer_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    geometry_uuid UUID NOT NULL
    REFERENCES loc.geometries (geometry_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    PRIMARY KEY (geometry_uuid, source_type_id, observer_uuid, observation_timestamp_utc)
)
PARTITION BY RANGE (observation_timestamp_utc);

/*
 * Manage partitions with pg_partman.
 * Highlights:
 * - `retention_keep_table = true`: detach old partitions instead of dropping them
 * - `infinite_time_partitions = true`: retain detached partitions indefinitely for processing
 */
SELECT partman.create_parent(
    p_parent_table => 'obs.observed_generation_values',
    p_control => 'observation_timestamp_utc',
    p_type => 'range',
    p_interval => '1 week',
    p_automatic_maintenance => 'on',
    p_jobmon => FALSE,
    p_premake => 7
);
UPDATE partman.part_config
SET
    retention = NULL,
    retention_keep_table = TRUE,
    retention_keep_index = TRUE,
    infinite_time_partitions = TRUE
WHERE parent_table = 'obs.observed_generation_values';
SELECT partman.run_maintenance('obs.observed_generation_values');
-- Schedule regular maintenance for the partitioned observed generation values table.
SELECT cron.schedule('partman-maintenance', '@hourly', $$CALL partman.run_maintenance_proc()$$);


/* == PREDICTIONS =================================================================================
 *
 * Schema and tables to handle predicted generation data.
 *
 * Predicted of generation values are produced by various forecast models for a specific location.
 * A forecast is a set of predicted generation values, beginning at the initialisation time. Each
 * subsequent generation's target time is equivalent to the initialisation time plus the horizon.
 *
 * The forecast produced most recently will likely be the most accurate.
 */

CREATE SCHEMA pred;

/*- Tables ----------------------------------------------------------------------------------*/

/*
 * A forecaster is a source that generates forecast values. This is usually an ML model,
 * but could also be an analytical process. Each forecaster's name and version number uniquely
 * identifies it.
 */
CREATE TABLE pred.forecasters (
    forecaster_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    forecaster_name TEXT NOT NULL,
    CONSTRAINT forecaster_name_format_check CHECK (
        LENGTH(forecaster_name) > 0 AND LENGTH(forecaster_name) < 64
        AND forecaster_name = LOWER(forecaster_name)
    ),
    forecaster_version TEXT NOT NULL,
    CONSTRAINT forecaster_version_format_check CHECK (
        LENGTH(forecaster_version) > 0 AND LENGTH(forecaster_version) < 64
        AND forecaster_version = LOWER(forecaster_version)
    ),
    created_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (forecaster_id),
    UNIQUE (forecaster_name, forecaster_version)
);

/*
 * Forecasts refer to the set of forecast values, created by a specific version of a forecaster,
 * for a specific location, with some initialization time. Each forecast contains a timeseries of
 * forecast values. There can only be one forecast per location per initialization time per
 * forecaster; reruns should replace old values.
 */
CREATE TABLE pred.forecasts (
    source_type_id SMALLINT NOT NULL
    REFERENCES loc.source_types (source_type_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
    value_resolution_mins SMALLINT NOT NULL,
    CONSTRAINT value_resolution_mins_size_check CHECK (
        value_resolution_mins > 0 AND value_resolution_mins <= 60
    ),
    forecaster_id INTEGER NOT NULL
    REFERENCES pred.forecasters (forecaster_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    created_at_utc TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    init_time_utc TIMESTAMP NOT NULL,
    geometry_uuid UUID NOT NULL
    REFERENCES loc.geometries (geometry_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    /* The forecast uuid should be generated using the init time as the time component */
    forecast_uuid UUID NOT NULL,
    target_period TSRANGE NOT NULL,
    CONSTRAINT target_period_valid_check CHECK (
        UPPER(target_period) > LOWER(target_period)
    ),
    CONSTRAINT target_period_recency_check CHECK (
        LOWER(target_period) >= '2000-01-01 00:00:00'::TIMESTAMP
    ),
    metadata JSONB DEFAULT NULL,
    p02_sips SMALLINT [],
    p10_sips SMALLINT [],
    p25_sips SMALLINT [],
    p50_sips SMALLINT [] NOT NULL,
    p75_sips SMALLINT [],
    p90_sips SMALLINT [],
    p98_sips SMALLINT [],
    CONSTRAINT plevel_lengths_match_check CHECK (
        ARRAY_LENGTH(p50_sips, 1) > 0
        AND COALESCE(ARRAY_LENGTH(p02_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        AND COALESCE(ARRAY_LENGTH(p10_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        AND COALESCE(ARRAY_LENGTH(p25_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        AND COALESCE(ARRAY_LENGTH(p75_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        AND COALESCE(ARRAY_LENGTH(p90_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
        AND COALESCE(ARRAY_LENGTH(p98_sips, 1), ARRAY_LENGTH(p50_sips, 1)) = ARRAY_LENGTH(p50_sips, 1)
    ),
    PRIMARY KEY (forecast_uuid)
)
PARTITION BY RANGE (forecast_uuid);

CREATE INDEX idx_forecasts_filter ON pred.forecasts (
    geometry_uuid,
    source_type_id,
    forecaster_id,
    forecast_uuid DESC
) INCLUDE (target_period);

/*
 * Manage partitions with pg_partman.
 * Highlights:
 * - `retention_keep_table = true`: detach old partitions instead of dropping them
 * - `infinite_time_partitions = true`: retain detached partitions indefinitely for processing
 */
SELECT partman.create_parent(
    p_parent_table => 'pred.forecasts',
    p_control => 'forecast_uuid',
    p_type => 'range',
    p_interval => '1 week',
    p_automatic_maintenance => 'on',
    p_jobmon => FALSE,
    p_premake => 7,
    p_time_encoder => 'partman.uuid7_time_encoder',
    p_time_decoder => 'partman.uuid7_time_decoder'
);
UPDATE partman.part_config
SET
    retention = NULL,
    retention_keep_table = TRUE,
    retention_keep_index = TRUE,
    infinite_time_partitions = TRUE
WHERE parent_table = 'pred.forecasts';
SELECT partman.run_maintenance('pred.forecasts');
SELECT cron.schedule('forecasts-vacuum', '30 4 * * *', $$VACUUM ANALYZE pred.forecasts$$);

/*
 * Procedure to cluster closed partitions of the forecasts table.
 * At write time, forecasts are naturally ordered on disk by forecast_uuid alone (which
 * corresponds to the init time of the forecast). However, the standard query route for
 * forecsats is to come through a geometry, source type, and forecaster first. There is
 * an index to speed this up, but it can be made even faster by physically co-locating
 * the data on disk to match the index. CLUSTER does this.
 */
-- +goose StatementBegin
CREATE PROCEDURE pred.cluster_closed_partitions(p_age INTERVAL DEFAULT '2 weeks')
LANGUAGE plpgsql AS $$
DECLARE
r RECORD;
BEGIN
PERFORM set_config('lock_timeout', '30s', FALSE);

FOR r IN
   SELECT c.relname, ci.relname AS index_name
   FROM pg_class AS c
       INNER JOIN pg_inherits AS inh ON inh.inhrelid = c.oid
       INNER JOIN pg_class AS p ON p.oid = inh.inhparent AND p.relname = 'forecasts'
       INNER JOIN pg_index AS i ON i.indrelid = c.oid
       INNER JOIN pg_class AS ci ON ci.oid = i.indexrelid
       INNER JOIN pg_inherits AS iinh ON iinh.inhrelid = i.indexrelid
       INNER JOIN pg_class AS pi ON pi.oid = iinh.inhparent AND pi.relname = 'idx_forecasts_filter'
   WHERE SUBSTRING(c.relname FROM 'p(\d{8})$')::DATE < CURRENT_DATE - p_age
       AND NOT i.indisclustered
LOOP
   BEGIN
       RAISE NOTICE 'clustering pred.%', r.relname;
       EXECUTE format('ALTER TABLE pred.%I CLUSTER ON %I', r.relname, r.index_name);
       EXECUTE format('CLUSTER pred.%I', r.relname);
       EXECUTE format('ANALYZE pred.%I', r.relname);
   EXCEPTION WHEN lock_not_available THEN
       RAISE WARNING 'skipping pred.%: could not acquire lock', r.relname;
   END;

   COMMIT;
END LOOP;
END $$;
-- +goose StatementEnd
SELECT cron.schedule('cluster-forecasts', '0 3 * * 0', $$CALL pred.cluster_closed_partitions()$$);


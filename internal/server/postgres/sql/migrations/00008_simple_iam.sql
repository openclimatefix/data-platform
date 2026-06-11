-- +goose Up

/*
 * Removes most of the functionality of the IAM setup.
 *
 * This is due to a new relience on an external system for user management. The only remaining
 * element relevant to the data platform is the organistion object.
 *
 * Note that the dropping of the IAM schema is permanent and not restored by the migration down.
 * It is empty in all deployed version of this application and so can and should be safely removed.
 */

DROP SCHEMA iam CASCADE;

CREATE TABLE IF NOT EXISTS loc.entities (
    entity_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    external_id TEXT NOT NULL
    CONSTRAINT external_id_format_check CHECK (
	external_id IS NOT NULL
	AND LENGTH(external_id) > 0
	AND LENGTH(external_id) <= 128
    ),
    UNIQUE (external_id)
);

ALTER TABLE loc.geometries
    ADD COLUMN owning_entity_id INTEGER DEFAULT NULL
    REFERENCES loc.entities(entity_id)
    ON UPDATE CASCADE
    ON DELETE SET NULL;

CREATE INDEX idx_owning_entity_id ON loc.geometries (owning_entity_id);

DROP MATERIALIZED VIEW IF EXISTS loc.sources_mv;
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
CREATE INDEX idx_sources_mv_gist_sys_period ON loc.sources_mv USING gist (sys_period);
CREATE INDEX idx_sources_mv_owning_entity_id ON loc.sources_mv (owning_entity_id);


-- +goose Down
DROP MATERIALIZED VIEW IF EXISTS loc.sources_mv;

ALTER TABLE loc.geometries
    DROP COLUMN owning_entity_id;

DROP TABLE IF EXISTS loc.entities;

CREATE MATERIALIZED VIEW loc.sources_mv AS
SELECT
    sh.geometry_uuid,
    sh.source_type_id,
    sh.capacity_watts,
    sh.capacity_limit_sip,
    sh.metadata,
    g.geometry_name,
    g.geometry_type_id,
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
CREATE UNIQUE INDEX ON loc.sources_mv (geometry_uuid, source_type_id, sys_period);
CREATE INDEX ON loc.sources_mv USING gist (sys_period);


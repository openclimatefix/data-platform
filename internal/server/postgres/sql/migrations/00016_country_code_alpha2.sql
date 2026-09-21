-- +goose Up

DROP MATERIALIZED VIEW loc.sources_mv;

ALTER TABLE loc.geometries DROP CONSTRAINT country_code_format_check;

UPDATE loc.geometries SET country_code = SUBSTRING(country_code, 1, 2)
WHERE country_code IS NOT NULL;

ALTER TABLE loc.geometries ALTER COLUMN country_code TYPE CHAR(2);

ALTER TABLE loc.geometries ADD CONSTRAINT country_code_format_check CHECK (country_code IS NULL OR country_code ~ '^[A-Z]{2}$');

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
    g.country_code,
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

CREATE UNIQUE INDEX ON loc.sources_mv (geometry_uuid, source_type_id, sys_period);
CREATE INDEX idx_sources_mv_owning_entity_id ON loc.sources_mv (owning_entity_id);
CREATE INDEX idx_sources_mv_composite_lookup ON loc.sources_mv USING gist (geometry_uuid, source_type_id, sys_period);
CREATE INDEX idx_sources_mv_country_type ON loc.sources_mv (country_code, geometry_type_id);

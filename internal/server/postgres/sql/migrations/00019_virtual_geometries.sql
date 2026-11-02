-- +goose Up

/* == COMPOSITE GEOMETRIES ========================================================================
 *
 * Table and materialized view updates to support composite geometries (linear weighted
 * combinations of source geometries).
 */

CREATE INDEX ON loc.geometries (geometry_name);

CREATE TABLE loc.geometry_components (
    geometry_uuid UUID NOT NULL
    REFERENCES loc.geometries (geometry_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    source_geometry_uuid UUID NOT NULL
    REFERENCES loc.geometries (geometry_uuid)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
    weight DOUBLE PRECISION NOT NULL,
    CONSTRAINT geometry_component_weight_positive_check CHECK (weight > 0),
    CONSTRAINT geometry_component_no_self_ref_check CHECK (
        geometry_uuid <> source_geometry_uuid
    ),
    PRIMARY KEY (geometry_uuid, source_geometry_uuid)
);

-- +goose StatementBegin
CREATE FUNCTION loc.check_geometry_components_not_nested() RETURNS TRIGGER
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM loc.geometry_components AS gc
        WHERE gc.geometry_uuid = NEW.source_geometry_uuid
            OR gc.source_geometry_uuid = NEW.geometry_uuid
    ) THEN
        RAISE EXCEPTION 'geometry components cannot be nested or circular'
            USING ERRCODE = '23503';
    END IF;
    RETURN NEW;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER trg_geometry_components_not_nested
BEFORE INSERT OR UPDATE ON loc.geometry_components
FOR EACH ROW EXECUTE FUNCTION loc.check_geometry_components_not_nested();

DROP MATERIALIZED VIEW loc.sources_mv;

CREATE MATERIALIZED VIEW loc.sources_mv AS
WITH physical_sources AS (
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
    INNER JOIN loc.geometries AS g USING (geometry_uuid)
),
cg_change_points AS (
    SELECT DISTINCT
        gc.geometry_uuid,
        ps.source_type_id,
        LOWER(ps.sys_period) AS valid_from_utc
    FROM loc.geometry_components AS gc
    INNER JOIN physical_sources AS ps
        ON ps.geometry_uuid = gc.source_geometry_uuid
        OR ps.geometry_uuid = gc.geometry_uuid
),
cg_periods AS (
    SELECT
        geometry_uuid,
        source_type_id,
        valid_from_utc,
        TSRANGE(
            valid_from_utc,
            LEAD(valid_from_utc, 1) OVER (
                PARTITION BY geometry_uuid, source_type_id
                ORDER BY valid_from_utc
            )
        ) AS sys_period
    FROM cg_change_points
),
composite_sources AS (
    SELECT
        cg.geometry_uuid,
        cgp.source_type_id,
        CASE
            WHEN COUNT(comp_ps.geometry_uuid) = COUNT(gc.source_geometry_uuid)
                THEN ROUND(SUM(gc.weight * comp_ps.capacity_watts))::BIGINT
            ELSE MAX(self_ps.capacity_watts)::BIGINT
        END AS capacity_watts,
        MAX(self_ps.capacity_limit_sip)::SMALLINT AS capacity_limit_sip,
        COALESCE(MAX(self_ps.metadata::TEXT)::JSONB, cg.metadata) AS metadata,
        COALESCE(MAX(self_ps.metadata_jsonb::TEXT)::JSONB, cg.metadata) AS metadata_jsonb,
        cg.geometry_name,
        cg.geometry_type_id,
        cg.country_code,
        cg.owning_entity_id,
        ST_X(cg.associated_point)::REAL AS longitude,
        ST_Y(cg.associated_point)::REAL AS latitude,
        cgp.sys_period,
        TRUE AS is_composite,
        ARRAY_AGG(gc.source_geometry_uuid ORDER BY gc.source_geometry_uuid)::UUID[] AS source_geometry_uuids,
        ARRAY_AGG(gc.weight ORDER BY gc.source_geometry_uuid)::DOUBLE PRECISION[] AS source_weights
    FROM cg_periods AS cgp
    INNER JOIN loc.geometries AS cg
        ON cg.geometry_uuid = cgp.geometry_uuid
    INNER JOIN loc.geometry_components AS gc
        ON gc.geometry_uuid = cgp.geometry_uuid
    LEFT JOIN physical_sources AS comp_ps
        ON comp_ps.geometry_uuid = gc.source_geometry_uuid
        AND comp_ps.source_type_id = cgp.source_type_id
        AND comp_ps.sys_period @> cgp.valid_from_utc
    LEFT JOIN physical_sources AS self_ps
        ON self_ps.geometry_uuid = cgp.geometry_uuid
        AND self_ps.source_type_id = cgp.source_type_id
        AND self_ps.sys_period @> cgp.valid_from_utc
    GROUP BY
        cg.geometry_uuid,
        cg.geometry_name,
        cg.geometry_type_id,
        cg.country_code,
        cg.owning_entity_id,
        cg.associated_point,
        cg.metadata,
        cgp.source_type_id,
        cgp.sys_period
    HAVING COUNT(comp_ps.geometry_uuid) = COUNT(gc.source_geometry_uuid)
        OR MAX(self_ps.capacity_watts) IS NOT NULL
)
SELECT
    ps.geometry_uuid,
    ps.source_type_id,
    ps.capacity_watts,
    ps.capacity_limit_sip,
    ps.metadata,
    ps.metadata_jsonb,
    ps.geometry_name,
    ps.geometry_type_id,
    ps.country_code,
    ps.owning_entity_id,
    ps.longitude,
    ps.latitude,
    ps.sys_period,
    FALSE::BOOLEAN AS is_composite,
    ARRAY[ps.geometry_uuid]::UUID[] AS source_geometry_uuids,
    ARRAY[1.0::DOUBLE PRECISION]::DOUBLE PRECISION[] AS source_weights
FROM physical_sources AS ps
WHERE NOT EXISTS (
    SELECT 1
    FROM loc.geometry_components AS gc
    WHERE gc.geometry_uuid = ps.geometry_uuid
)
UNION ALL
SELECT
    cs.geometry_uuid,
    cs.source_type_id,
    cs.capacity_watts,
    cs.capacity_limit_sip,
    cs.metadata,
    cs.metadata_jsonb,
    cs.geometry_name,
    cs.geometry_type_id,
    cs.country_code,
    cs.owning_entity_id,
    cs.longitude,
    cs.latitude,
    cs.sys_period,
    cs.is_composite::BOOLEAN AS is_composite,
    cs.source_geometry_uuids::UUID[] AS source_geometry_uuids,
    cs.source_weights::DOUBLE PRECISION[] AS source_weights
FROM composite_sources AS cs;

CREATE UNIQUE INDEX ON loc.sources_mv (geometry_uuid, source_type_id, sys_period);
CREATE INDEX idx_sources_mv_owning_entity_id ON loc.sources_mv (owning_entity_id);
CREATE INDEX idx_sources_mv_composite_lookup ON loc.sources_mv USING gist (geometry_uuid, source_type_id, sys_period);
CREATE INDEX idx_sources_mv_country_type ON loc.sources_mv (country_code, geometry_type_id);

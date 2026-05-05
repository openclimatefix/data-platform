-- +goose Up

-- Replace the old materialized view index for one suited to location-specific lookups
DROP INDEX IF EXISTS sources_mv_sys_period_idx;
CREATE INDEX idx_sources_mv_composite_gist ON loc.sources_mv USING gist (geometry_uuid, source_type_id, sys_period);

-- +goose Down
DROP INDEX IF EXISTS idx_sources_mv_composite_gist;
CREATE INDEX sources_mv_sys_period_idx ON loc.sources_mv USING gist (sys_period);

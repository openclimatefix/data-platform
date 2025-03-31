/*
Schema and tables to handle location-based data.

The generation data we store, be it predicted or otherwise, is always tied to a certain 
location. These locations vary in size and scope, from a single site to an entire country,
and the metadata we may want to store about them will also vary accordingly.

From an application standpoint, the location is pertinent in the case where we care about
the generated power as a fraction of the capacity of the location, as well as allowing us
to represent the data on a map.

In order to represent this supertype/subtype relationship, we will use a single table for the
supertype (location), plus a table for each subtype. This will allow us to more appropriately
represent the application.

https://stackoverflow.com/a/2672722

Having a few-column supertype location table allows for quicker querying of forecasts against
their given location. Materialized views then enable a easy way to get all the relevant data
for a given location type.
*/

-- +goose Up
-- +goose StatementBegin

CREATE SCHEMA loc;
COMMENT ON SCHEMA loc IS 'Locations schema';

/*- Lookups -----------------------------------------------------------------------------------*/

CREATE TABLE loc.energy_sources(
    source_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    source TEXT NOT NULL
        CHECK ( LENGTH(source) <= 15 ),
    PRIMARY KEY (id)
);
INSERT INTO loc.energy_sources (source) VALUES ('solar'), ('wind');
COMMENT ON TABLE loc.energy_sources IS 'Energy sources';
COMMENT ON COLUMN loc.energy_sources.source_id IS 'Unique identifier for energy source';
COMMENT ON COLUMN loc.energy_sources.source IS 'Representation of the energy source';


/*- Tables ----------------------------------------------------------------------------------*/

CREATE TABLE loc.locations (
    location_id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    name TEXT NOT NULL,
    latitude REAL NOT NULL
        CHECK ( latitude >= -90 AND latitude <= 90 ),
    longitude REAL NOT NULL
        CHECK ( longitude >= -180 AND longitude <= 180 ),
    capacity SMALLINT NOT NULL
        CHECK ( capacity_kw >= 0 ),
    capacity_unit_prefix_factor SMALLINT DEFAULT (0) NOT NULL
        CHECK ( unit_prefix_factor IN (0, 3, 6, 9, 12) ),
    sys_period TSTZRANGE NOT NULL DEFAULT TSTZRANGE(CURRENT_TIMESTAMP, NULL),
    PRIMARY KEY (location_id),
    UNIQUE (name, latitude, longitude, location_type)
);
COMMENT ON TABLE loc.locations IS 'Supertype table for locations.';
COMMENT ON COLUMN loc.locations.location_id IS 'Primary key for the location.';
COMMENT ON COLUMN loc.locations.name IS 'Name of the location.';
COMMENT ON COLUMN loc.locations.latitude IS 'Latitude associated with the location.';
COMMENT ON COLUMN loc.locations.longitude IS 'Longitude associated with the location.';
COMMENT ON COLUMN loc.locations.capacity IS 'Capacity of the location in factors of Watts. Multiply by 10 to the power of unit_prefix_factor to get the actual value.';
COMMENT ON COLUMN loc.locations.capacity_unit_prefix_factor IS 'Factor defining the metric prefix of the capacity value. Raise 10 to the power of this value to get the prefix.';

CREATE TABLE loc.locations_history(LIKE loc.locations);
COMMENT ON TABLE loc.locations_history IS 'History table for the locations supertype table.';
CREATE TRIGGER versioning_trigger
BEFORE INSERT OR UPDATE OR DELETE ON loc.locations
FOR EACH ROW EXECUTE PROCEDURE versioning(
  'sys_period', 'loc.locations_history', true
);

CREATE TABLE loc.site_metadata (
    location_id INTEGER NOT NULL
        REFERENCES loc.locations(location_id),
    client_name TEXT NOT NULL
        CHECK ( client_name IS NULL OR LENGTH(client_name) <= 64 ),
    client_site_id TEXT NOT NULL
        CHECK ( client_site_id IS NULL OR LENGTH(client_site_id) <= 126 ),
    yaw_degrees SMALLINT
        CHECK ( (yaw_degrees is NULL) or (yaw_degrees >= 0 AND yaw_degrees < 360) ),
    pitch_degrees SMALLINT
        CHECK ( (pitch_degrees is NULL) or (pitch_degrees >= 0 AND pitch_degrees <= 180) ),
    energy_source SMALLINT NOT NULL
        REFERENCES loc.energy_sources(id),
    PRIMARY KEY (location_id)
);
COMMENT ON TABLE loc.site_metadata IS 'Subtype table for site-level locations.
  These are typically single renewable generation sources identifiable via their lat/long.';
COMMENT ON COLUMN loc.site_metadata.location_id IS 'Foreign key to the location table.';
COMMENT ON COLUMN loc.site_metadata.client_name IS 'Name of the client associated with the site.';
COMMENT ON COLUMN loc.site_metadata.client_site_id IS 'ID of the site as given by the client.';
COMMENT ON COLUMN loc.site_metadata.yaw_degrees IS 'Yaw of the site in degrees (0: N, 90: E, 180: S, 270: W)';
COMMENT ON COLUMN loc.site_metadata.pitch_degrees IS 'Pitch of the site in degrees (0: Points directly downwards, 180: Points directly upwards)';

CREATE TABLE loc.region_metadata (
    location_id INTEGER NOT NULL
        REFERENCES loc.locations(location_id),
    region_name TEXT NOT NULL CHECK ( LENGTH(region_name) <= 64 ),
    boundary_geojson JSONB NOT NULL,
    PRIMARY KEY (location_id)
);
COMMENT ON TABLE loc.region_metadata IS 'Subtype table for region-level locations.
    These are bounded locations containing multiple sources such as DNOs or even whole countries.
    The supertype lat/long refers to their center point.';
COMMENT ON COLUMN loc.region_metadata.location_id IS 'Foreign key to the location table.';
COMMENT ON COLUMN loc.region_metadata.region_name IS 'Name of the region.';
COMMENT ON COLUMN loc.region_metadata.boundary_geojson IS 'GeoJSON representation of the region boundary.';

-- +goose StatementEnd


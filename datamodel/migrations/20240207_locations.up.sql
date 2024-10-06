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
*/

CREATE SCHEMA loc;
COMMENT ON SCHEMA loc IS 'Locations schema';

/*- Lookups -----------------------------------------------------------------------------------*/
CREATE TABLE loc.location_subtypes(
    id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    subtype TEXT NOT NULL
        CHECK ( LENGTH(subtype) <= 15 ),
    PRIMARY KEY (id)
);
INSERT INTO loc.location_subtypes (subtype) VALUES ('site'), ('region');
COMMENT ON TABLE loc.location_subtypes IS 'Location subtypes';
COMMENT ON COLUMN loc.location_subtypes.id IS 'Unique identifier for subtype';
COMMENT ON COLUMN loc.location_subtypes.subtype IS 'Representation of the subtype';

/*- Tables ----------------------------------------------------------------------------------*/

CREATE TABLE loc.locations (
    id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    name TEXT NOT NULL,
    latitude REAL NOT NULL
        CHECK ( latitude >= -90 AND latitude <= 90 ),
    longitude REAL NOT NULL
        CHECK ( longitude >= -180 AND longitude <= 180 ),
    capacity_kw INTEGER NOT NULL
        CHECK ( capacity_kw >= 0 ),
    location_type SMALLINT NOT NULL
        REFERENCES loc.location_subtypes(id),
    PRIMARY KEY (id),
    UNIQUE (name, latitude, longitude, location_type)
);
COMMENT ON TABLE loc.locations IS 'Supertype table for locations.';
COMMENT ON COLUMN loc.locations.id IS 'Primary key for the location.';
COMMENT ON COLUMN loc.locations.name IS 'Name of the location.';
COMMENT ON COLUMN loc.locations.latitude IS 'Latitude associated with the location.';
COMMENT ON COLUMN loc.locations.longitude IS 'Longitude associated with the location.';
COMMENT ON COLUMN loc.locations.capacity_kw IS 'Capacity of the location in kilowatts.';
COMMENT ON COLUMN loc.locations.location_type IS 'Type of location.';


CREATE TABLE loc.site_metadata (
    id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    location_id INTEGER NOT NULL
        REFERENCES loc.locations(id),
    client_name TEXT
        CHECK ( client_name IS NULL OR LENGTH(client_name) <= 64 ),
    client_site_id TEXT
        CHECK ( client_site_id IS NULL OR LENGTH(client_site_id) <= 126 ),
    created_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    yaw_degrees SMALLINT
        CHECK ( (yaw_degrees is NULL) or (yaw_degrees >= 0 AND yaw_degrees < 360) ),
    pitch_degrees SMALLINT
        CHECK ( (pitch_degrees is NULL) or (pitch_degrees >= 0 AND pitch_degrees <= 180) ),
    PRIMARY KEY (id)
);
COMMENT ON TABLE loc.site_metadata IS 'Subtype table for site-level locations.
 These are typically single renewable generation sources identifiable via their lat/long.';
COMMENT ON COLUMN loc.site_metadata.id IS 'Primary key for the site.';
COMMENT ON COLUMN loc.site_metadata.location_id IS 'Foreign key to the location table.';
COMMENT ON COLUMN loc.site_metadata.client_name IS 'Name of the client associated with the site.';
COMMENT ON COLUMN loc.site_metadata.client_site_id IS 'ID of the site as given by the client.';
COMMENT ON COLUMN loc.site_metadata.created_utc IS 'Timestamp of the creation of the site.';
COMMENT ON COLUMN loc.site_metadata.yaw_degrees IS 'Yaw of the site in degrees (0: N, 90: E, 180: S, 270: W)';
COMMENT ON COLUMN loc.site_metadata.pitch_degrees IS 'Pitch of the site in degrees (0: Points directly downwards, 180: Points directly upwards)';

CREATE TABLE loc.region_metadata (
    id INTEGER GENERATED ALWAYS AS IDENTITY NOT NULL,
    location_id INTEGER NOT NULL
        REFERENCES loc.locations(id),
    created_utc TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    region_name TEXT NOT NULL CHECK ( LENGTH(region_name) <= 64 ),
    boundary_geojson JSONB NOT NULL,
    PRIMARY KEY (id)
);
COMMENT ON TABLE loc.region_metadata IS 'Subtype table for region-level locations.
    These are bounded locations containing multiple sources such as DNOs or even whole countries.
    The supertype lat/long refers to their center point.';
COMMENT ON COLUMN loc.region_metadata.id IS 'Primary key for the region.';
COMMENT ON COLUMN loc.region_metadata.location_id IS 'Foreign key to the location table.';
COMMENT ON COLUMN loc.region_metadata.region_name IS 'Name of the region.';
COMMENT ON COLUMN loc.region_metadata.boundary_geojson IS 'GeoJSON representation of the region boundary.';


/*- Materialized Views ----------------------------------------------------------------------*/

CREATE MATERIALIZED VIEW loc.sites AS (
    SELECT 
        l.id AS location_id,
        l.name AS name,
        l.latitude AS latitude,
        l.longitude AS longitude,
        l.capacity_kw AS capacity_kw,
        s.id AS site_id,
        s.client_name AS client_name,
        s.client_site_id AS client_site_id,
        s.created_utc AS created_utc
    FROM loc.locations l
    INNER JOIN loc.site_metadata s ON l.id = s.location_id
);
COMMENT ON MATERIALIZED VIEW loc.sites IS 'Materialized view of the site locations and metadata.';

CREATE MATERIALIZED VIEW loc.regions AS (
    SELECT 
        l.id AS location_id,
        l.name AS name,
        l.latitude AS latitude,
        l.longitude AS longitude,
        l.capacity_kw AS capacity_kw,
        r.created_utc AS created_utc,
        r.id AS region_id
    FROM loc.locations l
    INNER JOIN loc.region_metadata r ON l.id = r.location_id
);
COMMENT ON MATERIALIZED VIEW loc.regions IS 'Materialized view of the region locations and metadata.';


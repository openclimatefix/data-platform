-- +goose Up

/*
 * Schema and tables to handle access management data.
 *
 * This schema isn't for storing any personally identifiable information; rather for detailing
 * roles and policies for user tokens and resources in the database.
 *
 * Roles are stored in a lookup table, and are used to determine the allowable
 * actions a user can take on a resource. These roles are then applied to users and
 * resources via policies. These policies are simply matchings between service accounts,
 * resource ids, and roles.
 */

CREATE SCHEMA iam;

/*- Lookups --------------------------------------------------------------------------------------*/

-- Lookup table to store the possible roles
CREATE TABLE iam.roles (
    role_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    role_name TEXT NOT NULL,
    CONSTRAINT role_name_format_check CHECK (
        LENGTH(role_name) > 0
        AND LENGTH(role_name) <= 64
        AND role_name = UPPER(role_name)
    ),
    PRIMARY KEY (role_id),
    UNIQUE (role_name)
);
INSERT INTO iam.roles (role_name) VALUES ('OWNER'), ('VIEWER');

/*- Tables --------------------------------------------------------------------------------------*/

/*
 * Table to store organizations.
 * An organization is a logical grouping of users. A user can only belong to one organisation.
 */
CREATE TABLE iam.orgs (
    org_uuid UUID DEFAULT UUIDV7() NOT NULL,
    org_name TEXT NOT NULL,
    CONSTRAINT org_name_format_check CHECK (
        LENGTH(org_name) > 0
        AND LENGTH(org_name) <= 128
        AND org_name = UPPER(org_name)
    ),
    metadata JSONB DEFAULT NULL,
    PRIMARY KEY (org_uuid),
    UNIQUE (org_name)
);

/*
 * Table to store users.
 * A user is identified by their oauth_id, which is a unique identifier served by the OAuth provider.
 * The oauth_id is not personally identifiable information, nor is it the primary key.
 * A user belongs to one organization, which defines their access policies.
 */
CREATE TABLE iam.users (
    user_uuid UUID DEFAULT UUIDV7() NOT NULL,
    org_uuid UUID NOT NULL
    REFERENCES iam.orgs (org_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    oauth_id TEXT NOT NULL,
    CONSTRAINT oauth_id_format_check CHECK (
        LENGTH(oauth_id) > 0
        AND LENGTH(oauth_id) <= 128
    ),
    metadata JSONB DEFAULT NULL,
    PRIMARY KEY (user_uuid),
    UNIQUE (oauth_id)
);
CREATE INDEX ON iam.users (oauth_id);
CREATE INDEX ON iam.users (org_uuid);

/*
 * Table to store logical groups of location policies.
 * This allows for easier assignment of the same set of policies to multiple orgs.
 */
CREATE TABLE iam.location_policy_groups (
    location_policy_group_uuid UUID DEFAULT UUIDV7() NOT NULL,
    location_policy_group_name TEXT NOT NULL,
    CONSTRAINT location_policy_group_name_format_check CHECK (
        LENGTH(location_policy_group_name) > 0
        AND LENGTH(location_policy_group_name) <= 128
        AND location_policy_group_name = UPPER(location_policy_group_name)
    ),
    PRIMARY KEY (location_policy_group_uuid),
    UNIQUE (location_policy_group_name)
);

/*
 * Pivot table to link orgs to location policy groups.
 * An org can belong to multiple location policy groups.
 */
CREATE TABLE iam.org_location_policy_groups (
    org_uuid UUID NOT NULL
    REFERENCES iam.orgs (org_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    location_policy_group_uuid UUID NOT NULL
    REFERENCES iam.location_policy_groups (location_policy_group_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    PRIMARY KEY (org_uuid, location_policy_group_uuid)
);

/*
 * Pivot table to define location policies.
 * These policies match locations to roles, and each policy is linked to a location group.
 * A location group can only have one role per location and source (can't be an OWNER *and* a
 * VIEWER for UK solar, for instance).
 */
CREATE TABLE iam.location_policies (
    role_id SMALLINT NOT NULL
    REFERENCES iam.roles (role_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
    source_type_id SMALLINT NOT NULL
    REFERENCES loc.source_types (source_type_id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT,
    location_uuid UUID NOT NULL
    REFERENCES loc.locations (location_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    location_policy_group_uuid UUID NOT NULL
    REFERENCES iam.location_policy_groups (location_policy_group_uuid)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    PRIMARY KEY (location_policy_group_uuid, location_uuid, source_type_id, role_id),
    UNIQUE (location_policy_group_uuid, location_uuid, source_type_id)
);

/*- Views ---------------------------------------------------------------------------------------*/

/*
 * View that presents org details in an aggregated format.
 */
CREATE OR REPLACE VIEW iam.org_details_v AS
WITH aggregated_policies AS (
    SELECT
        olpg.org_uuid,
        ARRAY_AGG(olpg.location_policy_group_uuid)::UUID[] AS location_policy_group_uuids,
        ARRAY_AGG(lpg.location_policy_group_name)::TEXT[] AS location_policy_group_names
    FROM iam.org_location_policy_groups AS olpg
    INNER JOIN iam.location_policy_groups AS lpg USING (location_policy_group_uuid)
    GROUP BY olpg.org_uuid
),
aggregated_users AS (
    SELECT
        u.org_uuid,
        ARRAY_AGG(u.user_uuid)::UUID[] AS user_uuids,
        ARRAY_AGG(u.oauth_id)::TEXT[] AS oauth_ids
    FROM iam.users AS u
    GROUP BY u.org_uuid
)
SELECT
    o.org_uuid,
    o.org_name,
    UUIDV7_EXTRACT_TIMESTAMP(o.org_uuid)::TIMESTAMP AS created_at_utc,
    o.metadata,
    ap.location_policy_group_uuids,
    ap.location_policy_group_names,
    au.user_uuids,
    au.oauth_ids
FROM iam.orgs AS o
    LEFT JOIN aggregated_policies AS ap USING (org_uuid)
    LEFT JOIN aggregated_users AS au USING (org_uuid)
ORDER BY o.org_name;

/*
 * Materialized view that denormalizes all of the IAM information into a single table. This enables
 * faster lookup when determining if a user has access to resources being called.
 */
CREATE MATERIALIZED VIEW iam.user_location_policies_mv AS
SELECT
    u.user_uuid,
    o.org_uuid,
    o.org_name,
    u.oauth_id,
    r.role_id,
    r.role_name,
    lp.location_uuid,
    lp.source_type_id
FROM iam.orgs AS o
    INNER JOIN iam.users AS u USING (org_uuid)
    INNER JOIN iam.org_location_policy_groups USING (org_uuid)
    INNER JOIN iam.location_policy_groups USING (location_policy_group_uuid)
    INNER JOIN iam.location_policies AS lp USING (location_policy_group_uuid)
    INNER JOIN iam.roles AS r USING (role_id)
ORDER BY u.user_uuid, r.role_id, lp.location_uuid, lp.source_type_id;
CREATE UNIQUE INDEX ON iam.user_location_policies_mv (user_uuid, role_id, location_uuid, source_type_id);

-- +goose Down
DROP SCHEMA iam CASCADE;

-- +goose Up

/*
 * Schema and tables to handle access management data.
 *
 * This schema isn't for storing any personally identifiable information; rather for detailing
 * permissions and policies for user tokens and resources in the database.
 *
 * Permissions are stored in a lookup table, and are used to determine the allowable
 * actions a user can take on a resource. These permissions are then applied to users and
 * resources via policies. These policies are simply matchings between service accounts,
 * resource ids, and permissions.
 */

CREATE SCHEMA iam;

/*- Lookups --------------------------------------------------------------------------------------*/

-- Lookup table to store the possible permissions
CREATE TABLE iam.permissions (
    permission_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    permission_name TEXT NOT NULL,
    CONSTRAINT permission_name_format_check CHECK (
        LENGTH(permission_name) > 0
        AND LENGTH(permission_name) <= 64
        AND permission_name = LOWER(permission_name)
    ),
    PRIMARY KEY (permission_id),
    UNIQUE (permission_name)
);
-- The ordering of these permissions matches the .proto enum definitions. Change with caution!
INSERT INTO iam.permissions (permission_name) VALUES ('read'), ('write');

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
        AND org_name = LOWER(org_name)
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
        AND location_policy_group_name = LOWER(location_policy_group_name)
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
 * These policies match locations to permissions, and each policy is linked to a location group.
 * A location group can only have one permission per location and source (can't be an OWNER *and* a
 * VIEWER for UK solar, for instance).
 */
CREATE TABLE iam.location_policies (
    permission_id SMALLINT NOT NULL
    REFERENCES iam.permissions (permission_id)
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
    PRIMARY KEY (location_policy_group_uuid, location_uuid, source_type_id, permission_id),
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


-- +goose Down
DROP SCHEMA iam CASCADE;

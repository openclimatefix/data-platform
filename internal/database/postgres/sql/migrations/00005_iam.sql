-- +goose Up

/*
 * Schema and tables to handle access management data.
 *
 * This schema isn't for storing any personally identifiable information; rather for detailing
 * roles and policies for user tokens and resources in the database.
 *
 * Roles are stored in a lookup table, and are used to determine the allowable
 * actions a user can take on a resource. These roles are then applied to users and 
 * resources via policies. These policies are simply matchings between user tokens,
 * resource ids, and roles.
 */

CREATE SCHEMA iam;

/*- Lookups -----------------------------------------------------------------------------------*/

-- Lookup table to store the user roles
CREATE TABLE iam.roles (
    role_id SMALLINT GENERATED ALWAYS AS IDENTITY NOT NULL,
    role_name TEXT NOT NULL
        CHECK (
            LENGTH(role_name) > 0
            AND LENGTH(role_name) <= 24
            AND role_name = UPPER(role_name)
        ),
    PRIMARY KEY (role_id),
    UNIQUE (role_name)
);
INSERT INTO iam.roles (role_name) VALUES ('OWNER'), ('VIEWER');


/*- Tables ----------------------------------------------------------------------------------*/

-- Pivot table to define location policies (match user tokens with locations and roles)
CREATE TABLE iam.location_policies (
    role_id SMALLINT NOT NULL
        REFERENCES iam.roles(role_id)
        ON DELETE RESTRICT,
    location_id INTEGER NOT NULL
        REFERENCES loc.locations(location_id)
        ON DELETE CASCADE,
    -- TODO: Determine with frontend what auth actually gets passed through
    user_token TEXT NOT NULL
        CHECK ( LENGTH(user_token) > 0 AND LENGTH(user_token) <= 64 ),
    PRIMARY KEY (user_token, location_id, role_id)
);

-- +goose Down
DROP SCHEMA iam CASCADE;

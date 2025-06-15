-- +goose Up

/* Functions to handle custom storing of wattages
 * Data is stored in the database as SMALLINT to keep storage down
**/

-- +goose StatementBegin
-- Function to convert a percent value to a SMALLINT representation
CREATE OR REPLACE FUNCTION encode_pct(value REAL)
RETURNS SMALLINT AS $$
DECLARE 
    max_value REAL := (32767::smallint * 100 / 30000.0); -- 109.3%
BEGIN
    IF value < 0.0 OR value > max_value THEN
        RAISE EXCEPTION 'Percentage value % must be in the range [0.0, 109.3]', value;
    END IF;
    RETURN CAST(value * 30000 / 100 AS SMALLINT);
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose StatementEnd

-- +goose StatementBegin
-- Function to decode a SMALLINT value back to a percent
CREATE OR REPLACE FUNCTION decode_smallint(value SMALLINT)
RETURNS REAL AS $$
BEGIN
    RETURN CAST(value AS REAL) * 100 / 30000.0;
END;
$$ LANGUAGE plpgsql IMMUTABLE;
-- +goose StatementEnd

-- +goose Down
DROP FUNCTION IF EXISTS encode_pct(REAL);
DROP FUNCTION IF EXISTS decode_smallint(SMALLINT);


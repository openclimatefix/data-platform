-- +goose Up

/*
 * Adds 4 new plevels to the predicted_generation_values table.
 */

ALTER TABLE pred.predicted_generation_values
    ADD COLUMN p02_sip SMALLINT,
    ADD CONSTRAINT p02_sip_nonnegative_check CHECK (p02_sip >= 0),
    ADD COLUMN p98_sip SMALLINT,
    ADD CONSTRAINT p98_sip_nonnegative_check CHECK (p98_sip >= 0),
    ADD COLUMN p25_sip SMALLINT,
    ADD CONSTRAINT p25_sip_nonnegative_check CHECK (p25_sip >= 0),
    ADD COLUMN p75_sip SMALLINT,
    ADD CONSTRAINT p75_sip_nonnegative_check CHECK (p75_sip >= 0);

-- +goose Down
ALTER TABLE pred.predicted_generation_values
    DROP COLUMN p02_sip,
    DROP COLUMN p25_sip,
    DROP COLUMN p75_sip;
    DROP COLUMN p98_sip,

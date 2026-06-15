-- Sets up ci_ack_durability with intentionally wrong data (5 rows instead of 60).
-- Running acked-write-durability-assert.sql after this MUST raise an exception.
CREATE TABLE IF NOT EXISTS ci_ack_durability(
    id        BIGSERIAL PRIMARY KEY,
    client_id INT  NOT NULL,
    op_id     INT  NOT NULL,
    payload   TEXT NOT NULL
);
TRUNCATE ci_ack_durability;
INSERT INTO ci_ack_durability(client_id, op_id, payload)
    SELECT 1, generate_series(1, 5), 'bad-data';

-- Seeds ci_tx_boundary with two autocommit rows where exactly one is required.
-- Running failover-commit-boundary-assert.sql after this MUST raise.
--
-- S1's assertion is deliberately loose on the `txn` side: 0 or 1 rows are both
-- legal, because a transaction interrupted by failover may commit or not. That
-- looseness is the whole risk — the only thing pinning the assertion down is
-- the autocommit count, and nothing proved that half could fail. A duplicated
-- autocommit row is the at-most-once violation it exists to catch.
CREATE TABLE IF NOT EXISTS ci_tx_boundary(
    id   BIGSERIAL PRIMARY KEY,
    mode TEXT NOT NULL
);
TRUNCATE ci_tx_boundary;
INSERT INTO ci_tx_boundary(mode) VALUES ('autocommit'), ('autocommit');

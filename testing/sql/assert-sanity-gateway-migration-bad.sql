-- Leaves ci_gateway_migration empty, which is the exact state the cluster is
-- in when the gateway severs every idle session instead of migrating one.
-- Running gateway-migration-assert.sql after this MUST raise: the zero-row
-- lower bound is the load-bearing half of that oracle, so it is the half whose
-- ability to fail has to be proven.
CREATE TABLE IF NOT EXISTS ci_gateway_migration(
    session_id  INT PRIMARY KEY,
    migrated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
TRUNCATE ci_gateway_migration;

-- Landing table for gateway-connection-survival. Each idle session held open
-- across the failover writes exactly one row AFTER the new leader is elected,
-- so the row count is an end-to-end record of how many sessions the gateway
-- carried across the leader change on their original client connection.
CREATE TABLE IF NOT EXISTS ci_gateway_migration(
    session_id  INT PRIMARY KEY,
    migrated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
TRUNCATE ci_gateway_migration;

-- Leaves ci_ddl_atomic in the exact half-committed shape W3 forbids: the table
-- present but without its PRIMARY KEY. Running ddl-failover-assert.sql after
-- this MUST raise.
--
-- W3's assertion accepts two outcomes (fully committed, fully absent), so it
-- passes on an empty database. Without this inversion, a ddl-failover run whose
-- DDL never executed at all would be indistinguishable from one that survived
-- the failover intact.
DROP INDEX IF EXISTS ci_ddl_idx;
DROP TABLE IF EXISTS ci_ddl_atomic;
CREATE TABLE ci_ddl_atomic(
    id      BIGINT NOT NULL,
    payload TEXT   NOT NULL
);

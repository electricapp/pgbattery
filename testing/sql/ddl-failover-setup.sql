-- Drop any leftover state from previous runs so the test is idempotent.
DROP TABLE IF EXISTS ci_ddl_atomic;
DROP INDEX  IF EXISTS ci_ddl_idx;

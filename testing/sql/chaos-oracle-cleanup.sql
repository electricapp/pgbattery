-- Drop the shared chaos-oracle table.
--
-- Run with `node: "leader"`, never a literal node. A case that moves
-- leadership leaves the node it started on as a standby, where DDL fails with
-- "cannot execute DROP TABLE in a read-only transaction" — a cleanup failure
-- that reports as residue when nothing is actually wrong.
DROP TABLE IF EXISTS ci_chaos_oracle;

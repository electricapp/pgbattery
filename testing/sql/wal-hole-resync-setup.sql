CREATE TABLE IF NOT EXISTS ci_wal_hole(
    seq     INT  PRIMARY KEY,
    payload TEXT NOT NULL
);
TRUNCATE ci_wal_hole;

-- Where node3 sits when it is about to be stopped. The fault is checked
-- against this: afterwards the leader must no longer hold the WAL segment
-- containing it, which is what makes the segment a hole node3 cannot cross.
CREATE TABLE IF NOT EXISTS ci_wal_hole_mark(lsn pg_lsn NOT NULL);
TRUNCATE ci_wal_hole_mark;
INSERT INTO ci_wal_hole_mark VALUES (pg_current_wal_lsn());

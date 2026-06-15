CREATE TABLE IF NOT EXISTS ci_wal_hole(
    seq     INT  PRIMARY KEY,
    payload TEXT NOT NULL
);
TRUNCATE ci_wal_hole;

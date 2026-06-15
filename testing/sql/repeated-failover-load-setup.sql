CREATE TABLE IF NOT EXISTS ci_failover_load(
    seq     INT  PRIMARY KEY,
    payload TEXT NOT NULL
);
TRUNCATE ci_failover_load;

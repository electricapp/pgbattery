CREATE TABLE IF NOT EXISTS ci_async_degraded(
    seq     INT  PRIMARY KEY,
    batch   TEXT NOT NULL
);
TRUNCATE ci_async_degraded;

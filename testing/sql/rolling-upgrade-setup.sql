CREATE TABLE IF NOT EXISTS ci_rolling_upgrade(
    seq     INT  PRIMARY KEY,
    batch   TEXT NOT NULL
);
TRUNCATE ci_rolling_upgrade;

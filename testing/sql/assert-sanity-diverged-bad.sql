-- Sets up ci_rejoin with 10 rows instead of the expected 25.
-- Running diverged-node-rejoin-assert.sql after this MUST raise an exception.
CREATE TABLE IF NOT EXISTS ci_rejoin(
    id  SERIAL PRIMARY KEY,
    val TEXT NOT NULL
);
TRUNCATE ci_rejoin;
INSERT INTO ci_rejoin(val) SELECT 'bad-' || generate_series(1, 10);

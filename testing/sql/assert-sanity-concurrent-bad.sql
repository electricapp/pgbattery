-- Sets up ci_concurrent_writes with a partial worker batch (500 rows, not 1000).
-- Running concurrent-writes-assert.sql after this MUST raise an exception.
CREATE TABLE IF NOT EXISTS ci_concurrent_writes(
    id        BIGSERIAL PRIMARY KEY,
    worker_id INT  NOT NULL,
    val       TEXT NOT NULL
);
TRUNCATE ci_concurrent_writes;
INSERT INTO ci_concurrent_writes(worker_id, val)
    SELECT 7, 'bad-' || generate_series(1, 500);

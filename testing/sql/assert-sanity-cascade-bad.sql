-- Seeds ci_concurrent_writes with a partial 150-row batch; a complete batch is
-- 200 rows. Running cascade-write-atomicity-assert.sql after this MUST raise.
--
-- DROP + CREATE rather than CREATE IF NOT EXISTS: assert-sanity-concurrent
-- shares this table name with a different column set (val TEXT instead of
-- seq INT), and this oracle groups on seq.
DROP TABLE IF EXISTS ci_concurrent_writes;
CREATE TABLE ci_concurrent_writes (
    id        SERIAL PRIMARY KEY,
    worker_id INT    NOT NULL,
    seq       INT    NOT NULL
);
INSERT INTO ci_concurrent_writes(worker_id, seq)
    SELECT 1, generate_series(1, 150);

CREATE TABLE IF NOT EXISTS ci_concurrent_writes (
    id        SERIAL PRIMARY KEY,
    worker_id INT    NOT NULL,
    seq       INT    NOT NULL
);
TRUNCATE ci_concurrent_writes;

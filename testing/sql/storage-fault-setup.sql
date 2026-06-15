CREATE TABLE IF NOT EXISTS ci_storage_fault(
    id      SERIAL PRIMARY KEY,
    payload TEXT NOT NULL
);
TRUNCATE ci_storage_fault;
INSERT INTO ci_storage_fault(payload)
    SELECT 'pre-fault-' || generate_series(1, 10);

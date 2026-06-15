CREATE TABLE IF NOT EXISTS ci_concurrent_backup(
    id      SERIAL PRIMARY KEY,
    payload TEXT NOT NULL
);
TRUNCATE ci_concurrent_backup;
DO $$
BEGIN
    FOR i IN 1..100 LOOP
        INSERT INTO ci_concurrent_backup(payload) VALUES ('row-' || i);
    END LOOP;
END $$;

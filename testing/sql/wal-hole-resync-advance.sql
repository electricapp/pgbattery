-- Generate ~20MB of WAL to exhaust the default wal_keep_size window,
-- then force segment boundaries so old segments can be recycled.
DO $$
BEGIN
    FOR i IN 1..80000 LOOP
        INSERT INTO ci_wal_hole(seq, payload) VALUES (i, repeat('x', 200));
    END LOOP;
END $$;
CHECKPOINT;
SELECT pg_switch_wal();
CHECKPOINT;
SELECT pg_switch_wal();
CHECKPOINT;

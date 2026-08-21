-- Generate ~20MB of WAL past the point node3 stopped at, then force the
-- segment boundaries and checkpoints that let the older ones be recycled.
-- Recycling is only possible because the preceding step removed both things
-- that were holding them: node3's replication slot and wal_keep_size.
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

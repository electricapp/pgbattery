-- Verifies that rows written before the storage fault survived and the
-- rejoin node (node3) has a consistent view via replication.
DO $$
DECLARE
    row_count INT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM ci_storage_fault;
    IF row_count < 10 THEN
        RAISE EXCEPTION
            'expected at least 10 rows after storage-fault recovery, got %', row_count;
    END IF;
END $$;

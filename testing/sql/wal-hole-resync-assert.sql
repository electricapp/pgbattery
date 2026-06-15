DO $$
DECLARE
    cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt FROM ci_wal_hole;
    IF cnt <> 80000 THEN
        RAISE EXCEPTION 'expected 80000 rows after WAL-hole resync, got % — node did not fully resync', cnt;
    END IF;
END $$;

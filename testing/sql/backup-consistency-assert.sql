DO $$
DECLARE
    row_count INT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM ci_backup_consistency;
    IF row_count <> 100 THEN
        RAISE EXCEPTION 'expected 100 rows, got %', row_count;
    END IF;
END $$;

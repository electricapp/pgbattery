DO $$
DECLARE
    row_count INT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM ci_restart_perm WHERE id = 1;
    IF row_count <> 1 THEN
        RAISE EXCEPTION 'expected 1 row, got %', row_count;
    END IF;
END $$;

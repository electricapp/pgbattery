DO $$
DECLARE
    row_count INT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM ci_rejoin;
    IF row_count <> 25 THEN
        RAISE EXCEPTION 'expected 25 rows, got %', row_count;
    END IF;
END $$;

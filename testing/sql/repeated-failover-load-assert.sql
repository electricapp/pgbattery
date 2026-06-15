DO $$
DECLARE
    total_rows INT;
BEGIN
    SELECT COUNT(*) INTO total_rows FROM ci_failover_load;
    IF total_rows <> 80 THEN
        RAISE EXCEPTION 'expected 80 rows after repeated failovers, got %', total_rows;
    END IF;
END $$;

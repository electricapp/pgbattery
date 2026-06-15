DO $$
DECLARE
    total_rows INT;
    fenced_rows INT;
BEGIN
    SELECT COUNT(*) INTO total_rows FROM ci_async_degraded;
    SELECT COUNT(*) INTO fenced_rows FROM ci_async_degraded WHERE batch = 'must-be-fenced';
    IF total_rows <> 40 THEN
        RAISE EXCEPTION 'expected 40 rows after recovery, got %', total_rows;
    END IF;
    IF fenced_rows <> 0 THEN
        RAISE EXCEPTION 'fenced write appeared in table: % rows with batch=must-be-fenced', fenced_rows;
    END IF;
END $$;

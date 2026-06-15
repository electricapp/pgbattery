DO $$
DECLARE
    total_rows INT;
    dup_rows   INT;
BEGIN
    SELECT COUNT(*)                          INTO total_rows FROM ci_rogue_promote;
    SELECT COUNT(*) - COUNT(DISTINCT id)     INTO dup_rows   FROM ci_rogue_promote;

    -- At least the 20 pre-promote rows must be present; no duplicates allowed.
    IF total_rows < 20 THEN
        RAISE EXCEPTION 'expected at least 20 rows after rogue promote test, got %', total_rows;
    END IF;
    IF dup_rows <> 0 THEN
        RAISE EXCEPTION 'duplicate rows detected after rogue promote: % duplicates', dup_rows;
    END IF;
END $$;

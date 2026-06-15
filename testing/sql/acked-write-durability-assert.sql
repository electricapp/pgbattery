DO $$
DECLARE
    total_rows INT;
    dup_rows   INT;
BEGIN
    SELECT COUNT(*) INTO total_rows FROM ci_ack_durability;
    SELECT COUNT(*) - COUNT(DISTINCT (client_id, op_id))
        INTO dup_rows FROM ci_ack_durability;

    IF total_rows <> 60 THEN
        RAISE EXCEPTION 'expected 60 rows, got %', total_rows;
    END IF;
    IF dup_rows <> 0 THEN
        RAISE EXCEPTION 'duplicate rows detected: %', dup_rows;
    END IF;
END $$;

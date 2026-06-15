-- Asserts the seed row planted in -setup.sql survived the leadership
-- transfer. A graceful transfer must preserve all acknowledged writes;
-- otherwise the new leader was promoted before it had caught up.
DO $$
DECLARE
    row_count INT;
    marker_val TEXT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM ci_leadership_transfer_survival;
    IF row_count <> 1 THEN
        RAISE EXCEPTION 'leadership-transfer data oracle: expected 1 row, got %', row_count;
    END IF;
    SELECT marker INTO marker_val
        FROM ci_leadership_transfer_survival
        WHERE case_run_id = 'leadership-transfer';
    IF marker_val <> 'survived-the-transfer' THEN
        RAISE EXCEPTION 'leadership-transfer data oracle: marker corrupted, got %', marker_val;
    END IF;
END $$;

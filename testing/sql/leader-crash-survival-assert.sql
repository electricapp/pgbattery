-- Asserts the seed row planted in -setup.sql survived the leader kill.
-- Failover must preserve acknowledged writes; if this row vanished, the
-- new leader was promoted from a divergent / behind replica.
DO $$
DECLARE
    row_count INT;
    marker_val TEXT;
BEGIN
    SELECT COUNT(*) INTO row_count FROM ci_leader_crash_survival;
    IF row_count <> 1 THEN
        RAISE EXCEPTION 'leader-crash data oracle: expected 1 row, got %', row_count;
    END IF;
    SELECT marker INTO marker_val
        FROM ci_leader_crash_survival
        WHERE case_run_id = 'leader-crash';
    IF marker_val <> 'survived-the-kill' THEN
        RAISE EXCEPTION 'leader-crash data oracle: marker corrupted, got %', marker_val;
    END IF;
END $$;

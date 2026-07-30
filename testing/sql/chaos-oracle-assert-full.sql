-- Asserts all three phases of the shared chaos oracle. Paired with faults that
-- provably retain quorum for their whole window, so the mid-fault batch is
-- genuinely expected to be acknowledged rather than indeterminate: 100 rows
-- acked before the fault, 25 acked while the fault was active, 50 acked after
-- the heal — each exactly once.
DO $$
DECLARE
    pre_rows   INT;
    mid_rows   INT;
    post_rows  INT;
    other_rows INT;
    dup_rows   INT;
BEGIN
    SELECT COUNT(*) INTO pre_rows   FROM ci_chaos_oracle WHERE phase = 'pre';
    SELECT COUNT(*) INTO mid_rows   FROM ci_chaos_oracle WHERE phase = 'mid';
    SELECT COUNT(*) INTO post_rows  FROM ci_chaos_oracle WHERE phase = 'post';
    SELECT COUNT(*) INTO other_rows FROM ci_chaos_oracle WHERE phase NOT IN ('pre', 'mid', 'post');
    SELECT COUNT(*) - COUNT(DISTINCT (phase, seq)) INTO dup_rows FROM ci_chaos_oracle;

    IF pre_rows <> 100 THEN
        RAISE EXCEPTION
            'chaos oracle: expected 100 acked pre-fault rows, got % — acknowledged writes were lost', pre_rows;
    END IF;
    IF mid_rows <> 25 THEN
        RAISE EXCEPTION
            'chaos oracle: expected 25 rows acked during the fault, got % — a write acked under the fault was lost', mid_rows;
    END IF;
    IF post_rows <> 50 THEN
        RAISE EXCEPTION
            'chaos oracle: expected 50 post-recovery rows, got % — the healed cluster did not commit the batch', post_rows;
    END IF;
    IF dup_rows <> 0 THEN
        RAISE EXCEPTION 'chaos oracle: % duplicate rows detected', dup_rows;
    END IF;
    IF other_rows <> 0 THEN
        RAISE EXCEPTION 'chaos oracle: % rows in an unknown phase', other_rows;
    END IF;
END $$;

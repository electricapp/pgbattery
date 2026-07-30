-- Asserts both halves of the shared chaos oracle: the 100 rows acked before
-- the fault survived exactly once each (W1/W2), and the 50 rows acked after
-- the cluster reconverged committed in full — proving the healed cluster
-- accepts writes, not merely that it reports one leader.
DO $$
DECLARE
    pre_rows   INT;
    post_rows  INT;
    other_rows INT;
    dup_rows   INT;
BEGIN
    SELECT COUNT(*) INTO pre_rows   FROM ci_chaos_oracle WHERE phase = 'pre';
    SELECT COUNT(*) INTO post_rows  FROM ci_chaos_oracle WHERE phase = 'post';
    SELECT COUNT(*) INTO other_rows FROM ci_chaos_oracle WHERE phase NOT IN ('pre', 'post');
    SELECT COUNT(*) - COUNT(DISTINCT (phase, seq)) INTO dup_rows FROM ci_chaos_oracle;

    IF pre_rows <> 100 THEN
        RAISE EXCEPTION
            'chaos oracle: expected 100 acked pre-fault rows, got % — acknowledged writes were lost', pre_rows;
    END IF;
    IF post_rows <> 50 THEN
        RAISE EXCEPTION
            'chaos oracle: expected 50 post-recovery rows, got % — the healed cluster did not commit the batch', post_rows;
    END IF;
    IF dup_rows <> 0 THEN
        RAISE EXCEPTION 'chaos oracle: % duplicate rows detected', dup_rows;
    END IF;
    IF other_rows <> 0 THEN
        RAISE EXCEPTION
            'chaos oracle: % rows outside the pre/post phases — this assertion is paired with cases that write no mid batch', other_rows;
    END IF;
END $$;

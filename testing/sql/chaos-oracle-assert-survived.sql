-- Asserts the 100 acked rows seeded by chaos-oracle-setup.sql survived the
-- fault exactly once each.
--
-- For cases that deliberately stop short of full replication health, where a
-- post-recovery write could block on a standby that is still catching up. The
-- `post`/`mid` phases must be empty here: this file is paired only with cases
-- that issue neither, so any row in them means the oracle was mis-wired and is
-- no longer checking what it claims.
DO $$
DECLARE
    pre_rows   INT;
    other_rows INT;
    dup_rows   INT;
BEGIN
    SELECT COUNT(*) INTO pre_rows   FROM ci_chaos_oracle WHERE phase = 'pre';
    SELECT COUNT(*) INTO other_rows FROM ci_chaos_oracle WHERE phase <> 'pre';
    SELECT COUNT(*) - COUNT(DISTINCT (phase, seq)) INTO dup_rows FROM ci_chaos_oracle;

    IF pre_rows <> 100 THEN
        RAISE EXCEPTION
            'chaos oracle: expected 100 acked pre-fault rows, got % — acknowledged writes were lost', pre_rows;
    END IF;
    IF dup_rows <> 0 THEN
        RAISE EXCEPTION 'chaos oracle: % duplicate rows detected', dup_rows;
    END IF;
    IF other_rows <> 0 THEN
        RAISE EXCEPTION
            'chaos oracle: % rows outside the pre phase — this assertion is paired with cases that write no mid/post batch', other_rows;
    END IF;
END $$;

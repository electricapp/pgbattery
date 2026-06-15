DO $$
DECLARE
    total_rows INT;
    gap_count  INT;
BEGIN
    SELECT COUNT(*) INTO total_rows FROM ci_rolling_upgrade;
    -- Verify no gaps: every seq 1..60 must be present
    SELECT COUNT(*) INTO gap_count
    FROM generate_series(1, 60) AS g(n)
    WHERE NOT EXISTS (SELECT 1 FROM ci_rolling_upgrade WHERE seq = g.n);

    IF total_rows <> 60 THEN
        RAISE EXCEPTION 'expected 60 rows after rolling upgrade, got %', total_rows;
    END IF;
    IF gap_count <> 0 THEN
        RAISE EXCEPTION '% gaps found in seq 1..60 after rolling upgrade', gap_count;
    END IF;
END $$;

DO $$
BEGIN
    FOR i IN 1..30 LOOP
        INSERT INTO ci_rolling_upgrade(seq, batch) VALUES (i, 'b1');
    END LOOP;
END $$;

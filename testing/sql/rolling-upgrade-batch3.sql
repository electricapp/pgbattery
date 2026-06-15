DO $$
BEGIN
    FOR i IN 41..50 LOOP
        INSERT INTO ci_rolling_upgrade(seq, batch) VALUES (i, 'b3');
    END LOOP;
END $$;

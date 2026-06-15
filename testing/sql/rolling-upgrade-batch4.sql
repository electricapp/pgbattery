DO $$
BEGIN
    FOR i IN 51..60 LOOP
        INSERT INTO ci_rolling_upgrade(seq, batch) VALUES (i, 'b4');
    END LOOP;
END $$;

DO $$
BEGIN
    FOR i IN 31..40 LOOP
        INSERT INTO ci_rolling_upgrade(seq, batch) VALUES (i, 'b2');
    END LOOP;
END $$;

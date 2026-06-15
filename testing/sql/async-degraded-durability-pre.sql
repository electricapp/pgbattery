DO $$
BEGIN
    FOR i IN 1..30 LOOP
        INSERT INTO ci_async_degraded(seq, batch) VALUES (i, 'pre');
    END LOOP;
END $$;

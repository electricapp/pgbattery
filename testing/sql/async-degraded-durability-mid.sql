DO $$
BEGIN
    FOR i IN 31..40 LOOP
        INSERT INTO ci_async_degraded(seq, batch) VALUES (i, 'mid');
    END LOOP;
END $$;

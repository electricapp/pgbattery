DO $$
BEGIN
    FOR i IN 21..40 LOOP
        INSERT INTO ci_failover_load(seq, payload)
        VALUES (i, 'b2') ON CONFLICT DO NOTHING;
    END LOOP;
END $$;

DO $$
BEGIN
    FOR i IN 41..60 LOOP
        INSERT INTO ci_failover_load(seq, payload)
        VALUES (i, 'b3') ON CONFLICT DO NOTHING;
    END LOOP;
END $$;

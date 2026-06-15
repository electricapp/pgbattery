DO $$
BEGIN
    FOR i IN 1..20 LOOP
        INSERT INTO ci_failover_load(seq, payload)
        VALUES (i, 'b1') ON CONFLICT DO NOTHING;
    END LOOP;
END $$;

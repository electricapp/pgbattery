DO $$
BEGIN
    FOR i IN 1..10 LOOP
        INSERT INTO ci_network_latency(seq) VALUES (i) ON CONFLICT DO NOTHING;
    END LOOP;
END $$;

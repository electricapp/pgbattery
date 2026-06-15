DO $$
BEGIN
    FOR i IN 61..80 LOOP
        INSERT INTO ci_failover_load(seq, payload)
        VALUES (i, 'b4') ON CONFLICT DO NOTHING;
    END LOOP;
END $$;

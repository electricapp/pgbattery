DO $$
BEGIN
    FOR i IN 1..25 LOOP
        INSERT INTO ci_rejoin(seq) VALUES (i) ON CONFLICT DO NOTHING;
    END LOOP;
END $$;

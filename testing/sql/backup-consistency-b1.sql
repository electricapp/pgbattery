DO $$
BEGIN
    FOR i IN 1..50 LOOP
        INSERT INTO ci_backup_consistency(seq) VALUES (i)
        ON CONFLICT DO NOTHING;
    END LOOP;
END $$;

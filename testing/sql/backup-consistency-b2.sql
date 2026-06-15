DO $$
BEGIN
    FOR i IN 51..100 LOOP
        INSERT INTO ci_backup_consistency(seq) VALUES (i)
        ON CONFLICT DO NOTHING;
    END LOOP;
END $$;

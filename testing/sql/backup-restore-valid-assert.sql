DO $$
DECLARE
    total_rows INT;
    post_rows  INT;
BEGIN
    SELECT COUNT(*) INTO total_rows FROM ci_backup_restore;
    SELECT COUNT(*) INTO post_rows  FROM ci_backup_restore WHERE marker LIKE 'post-backup%';
    IF total_rows <> 3 THEN
        RAISE EXCEPTION 'expected 3 rows after restore, got %', total_rows;
    END IF;
    IF post_rows <> 0 THEN
        RAISE EXCEPTION 'post-backup rows survived restore: % rows', post_rows;
    END IF;
END $$;

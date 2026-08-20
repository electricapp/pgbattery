-- The live cluster is unmoved by a standby's restore.
DO $$
DECLARE
    total_rows INT;
    post_rows  INT;
BEGIN
    SELECT COUNT(*) INTO total_rows FROM ci_backup_restore;
    SELECT COUNT(*) INTO post_rows  FROM ci_backup_restore WHERE marker LIKE 'post-backup%';
    IF total_rows <> 4 THEN
        RAISE EXCEPTION 'leader holds % rows, expected 4 (3 seeded + 1 post-backup)', total_rows;
    END IF;
    IF post_rows <> 1 THEN
        RAISE EXCEPTION 'the post-backup row was rolled back by a restore on another node';
    END IF;
END $$;

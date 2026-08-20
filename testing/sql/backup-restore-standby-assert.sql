-- What a restored node is, run against its own PostgreSQL rather than a
-- gateway. A full restore unpacks pg_basebackup output, which carries no
-- standby.signal; without the one the restore writes, this node would open its
-- restored snapshot as a writable primary on its own timeline and the cluster
-- would have two write authorities.
DO $$
DECLARE
    in_recovery BOOLEAN;
    total_rows  INT;
BEGIN
    SELECT pg_is_in_recovery() INTO in_recovery;
    IF NOT in_recovery THEN
        RAISE EXCEPTION 'the restored node opened its snapshot as a writable primary';
    END IF;
    SELECT COUNT(*) INTO total_rows FROM ci_backup_restore;
    IF total_rows <> 4 THEN
        RAISE EXCEPTION
            'restored node holds % rows, expected 4: it follows the leader rather than '
            'staying pinned to the 3-row snapshot it was restored from', total_rows;
    END IF;
END $$;

-- Rows inserted after the backup snapshot; must disappear after restore.
INSERT INTO ci_backup_restore(marker) VALUES ('post-backup-should-vanish');

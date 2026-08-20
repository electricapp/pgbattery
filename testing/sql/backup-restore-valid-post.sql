-- Written after the backup snapshot. A restore is a node-local operation, so
-- this row must still be here afterwards: rolling the live cluster back to a
-- standby's restored snapshot would be a lost acked write.
INSERT INTO ci_backup_restore(marker) VALUES ('post-backup-must-survive');

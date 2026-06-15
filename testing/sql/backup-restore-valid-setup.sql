CREATE TABLE IF NOT EXISTS ci_backup_restore(
    id      SERIAL PRIMARY KEY,
    marker  TEXT NOT NULL
);
TRUNCATE ci_backup_restore;
INSERT INTO ci_backup_restore(marker) VALUES ('pre-backup-1');
INSERT INTO ci_backup_restore(marker) VALUES ('pre-backup-2');
INSERT INTO ci_backup_restore(marker) VALUES ('pre-backup-3');

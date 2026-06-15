-- Data-integrity oracle for the leadership-transfer case.
-- Verifies a graceful (non-crashing) leadership transfer preserves
-- acknowledged writes.
CREATE TABLE IF NOT EXISTS ci_leadership_transfer_survival(
    case_run_id TEXT PRIMARY KEY,
    marker      TEXT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
TRUNCATE ci_leadership_transfer_survival;
INSERT INTO ci_leadership_transfer_survival (case_run_id, marker)
VALUES ('leadership-transfer', 'survived-the-transfer');

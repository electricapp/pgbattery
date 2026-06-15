-- Data-integrity oracle for the leader-crash case.
-- Seeds a known marker row before the kill so the post-failover assertion
-- can verify the write survived. Run against the current leader's gateway.
CREATE TABLE IF NOT EXISTS ci_leader_crash_survival(
    case_run_id TEXT PRIMARY KEY,
    marker      TEXT NOT NULL,
    inserted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
TRUNCATE ci_leader_crash_survival;
INSERT INTO ci_leader_crash_survival (case_run_id, marker)
VALUES ('leader-crash', 'survived-the-kill');

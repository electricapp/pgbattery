-- Shared data oracle for the heavyweight chaos cases.
--
-- The 100-row INSERT is a single implicit transaction, and the runner runs
-- this file with ON_ERROR_STOP=1 and expect_exit 0 — so a successful setup
-- step is proof that all 100 rows were ACKNOWLEDGED writes. Every one of them
-- must therefore reappear exactly once once the fault has healed (W1), with no
-- duplicates (W2).
--
-- TRUNCATE makes each case self-contained inside a reuse_cluster suite.
-- `id` (not `(phase, seq)`) is the primary key so a duplicated row is
-- representable in the table and the assertion can count it, exactly as
-- ci_ack_durability does.
CREATE TABLE IF NOT EXISTS ci_chaos_oracle(
    id    BIGSERIAL PRIMARY KEY,
    phase TEXT NOT NULL,
    seq   INT  NOT NULL
);
TRUNCATE ci_chaos_oracle;
INSERT INTO ci_chaos_oracle(phase, seq)
    SELECT 'pre', generate_series(1, 100);

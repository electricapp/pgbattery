-- Seeds ci_chaos_oracle with 5 pre rows instead of the 100 that
-- chaos-oracle-setup.sql acknowledges, and with no mid/post batch at all.
-- Running any of chaos-oracle-assert-survived.sql,
-- chaos-oracle-assert-survived-and-writable.sql, or
-- chaos-oracle-assert-full.sql after this MUST raise an exception.
CREATE TABLE IF NOT EXISTS ci_chaos_oracle(
    id    BIGSERIAL PRIMARY KEY,
    phase TEXT NOT NULL,
    seq   INT  NOT NULL
);
TRUNCATE ci_chaos_oracle;
INSERT INTO ci_chaos_oracle(phase, seq)
    SELECT 'pre', generate_series(1, 5);

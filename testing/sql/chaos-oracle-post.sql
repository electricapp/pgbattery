-- Post-recovery acked batch for the shared chaos oracle.
--
-- Run after the cluster has reconverged with full replication health. It
-- proves the healed cluster is genuinely WRITABLE, which "leaders: 1" does
-- not: a promoted node whose sync replication never came back reports one
-- leader while every commit blocks forever.
INSERT INTO ci_chaos_oracle(phase, seq)
    SELECT 'post', generate_series(1, 50);

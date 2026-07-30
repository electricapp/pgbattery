-- Mid-fault acked batch for the shared chaos oracle.
--
-- Only for cases where quorum is provably retained for the whole fault window
-- (clock skew on a single node, packet loss, one node out of disk) so the
-- commit is genuinely expected to be acknowledged. Cases that lose quorum use
-- the pre/post phases only — a mid batch there would be legitimately
-- indeterminate, and asserting on it would flake.
INSERT INTO ci_chaos_oracle(phase, seq)
    SELECT 'mid', generate_series(1, 25);

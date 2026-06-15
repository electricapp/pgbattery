-- This INSERT must fail because the cluster has no quorum.
INSERT INTO ci_async_degraded(seq, batch) VALUES (99, 'must-be-fenced');

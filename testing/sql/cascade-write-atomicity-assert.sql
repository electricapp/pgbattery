-- Batch-atomicity oracle for the sustained write load in
-- cascade-double-failover-wedge.
--
-- Each background worker fires `INSERT INTO ci_concurrent_writes(worker_id,seq)
-- SELECT $w, generate_series((($i-1)*200)+1, $i*200)` — one statement, so one
-- implicit transaction, so one atomic 200-row batch. Whether a given batch was
-- acknowledged is genuinely indeterminate under three cascading leader kills,
-- so the defensible property is per-batch atomicity rather than a total count:
-- a batch that appears at all must contain exactly 200 rows. 1..199 rows means
-- a transaction partially materialised; more than 200 means a duplicate replay.
--
-- The final check keeps the oracle from passing vacuously: if the fault
-- sequence starved the workload of every single commit there is nothing to
-- verify, and a silently-empty table must not read as a pass.
DO $$
DECLARE
    r                RECORD;
    complete_batches INT := 0;
BEGIN
    FOR r IN
        SELECT worker_id,
               ((seq - 1) / 200) + 1 AS batch,
               count(*)              AS cnt
          FROM ci_concurrent_writes
         GROUP BY 1, 2
         ORDER BY 1, 2
    LOOP
        IF r.cnt <> 200 THEN
            RAISE EXCEPTION
                'cascade oracle: worker % batch % holds % rows (a batch is one transaction: 0 or 200) — atomicity violated',
                r.worker_id, r.batch, r.cnt;
        END IF;
        complete_batches := complete_batches + 1;
    END LOOP;

    IF complete_batches = 0 THEN
        RAISE EXCEPTION
            'cascade oracle: not one write batch survived the cascade — the workload committed nothing, so this case verified nothing';
    END IF;
END $$;

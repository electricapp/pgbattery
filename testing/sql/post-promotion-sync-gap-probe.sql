-- RW-2 probe. Runs on a standby that may be promoted, in a session opened
-- before the failover so the write costs no connect round trip.
--
-- The wait is on protocol state — pg_is_in_recovery() flipping false — not on
-- a sleep, so the INSERT lands in the post-promotion window however long or
-- short that window turns out to be on the day.
--
-- Every marker is proof of what it names. The first column is derived from
-- pg_is_in_recovery() at the moment it prints, so a probe on the node that was
-- never promoted says NOTPROMOTED and can never be mistaken for a verdict.
-- statement_timeout is deliberately not set until after the wait: bounding the
-- wait loop with it would cancel the loop and leave the following statements
-- reporting a promotion that never happened.
--
-- Markers:
--   PROMOTED|<clock>|<synchronous_standby_names>|<connected standbys>|<read_only>
--   NOTPROMOTED|<clock>|...        (this node stayed a standby; not a verdict)
--   ACKED|<clock>|<commit lsn>|<synchronous_standby_names at ack>|<read_only>
--   SYNCNOW|<standbys designated sync at ack — the durability answer>
--   SYNCACK|<sync standbys whose flush_lsn covers the commit — corroboration>
-- A commit that never acknowledges produces no ACKED line; psql reports the
-- statement timeout instead — the safe outcome, and the point of the test.

\set ON_ERROR_STOP off
\timing off
\pset tuples_only on
\pset format unaligned
\pset fieldsep '|'

-- Park until this node is primary. The loop carries its own deadline so it
-- ends on its own terms rather than being cancelled mid-flight; pg_sleep keeps
-- the poll off a spin loop, and 2 ms is far finer than the window measured on
-- a live cluster (~120 ms).
DO $$
DECLARE
    deadline timestamptz := clock_timestamp() + interval '90 seconds';
BEGIN
    LOOP
        EXIT WHEN NOT pg_is_in_recovery();
        EXIT WHEN clock_timestamp() > deadline;
        PERFORM pg_sleep(0.002);
    END LOOP;
END $$;

-- The instant of promotion and the sync configuration in force right now. An
-- empty list on a node that is out of recovery is RW-2 open.
SELECT CASE WHEN pg_is_in_recovery() THEN 'NOTPROMOTED' ELSE 'PROMOTED' END,
       clock_timestamp(),
       current_setting('synchronous_standby_names'),
       (SELECT count(*) FROM pg_stat_replication),
       -- Both flags: `default_transaction_read_only` is what the supervisor
       -- writes, `transaction_read_only` is what actually governs this
       -- statement. They can disagree, and only the second one refuses a write.
       current_setting('default_transaction_read_only')
         || '/' || current_setting('transaction_read_only');

-- Bound the commit only. A commit that waits this long has demonstrably
-- refused to acknowledge without a standby, which is the passing behaviour.
SET statement_timeout = '20s';

-- The commit under test. The LSN comes back from the INSERT itself, not from a
-- following statement: a refused write leaves `commit_lsn` unset, so the ACKED
-- marker below cannot print. Read separately it would still have succeeded —
-- a read-only transaction happily runs a SELECT — and the probe would announce
-- an acknowledgement for a commit that never happened.
INSERT INTO rw2_probe(tag) VALUES ('gap-write')
RETURNING pg_current_wal_lsn() AS commit_lsn \gset

SELECT 'ACKED', clock_timestamp(), :'commit_lsn',
       current_setting('synchronous_standby_names'),
       current_setting('default_transaction_read_only');

-- Was a synchronous standby designated when the commit ran? With
-- synchronous_commit = on this is the durability answer: PostgreSQL does not
-- return from a commit until a designated sync standby has flushed it, so a
-- standby in sync_state = 'sync' means the acknowledgement was backed. Zero
-- means the primary acknowledged a write only it held — RW-2 open.
SELECT 'SYNCNOW', count(*) FROM pg_stat_replication WHERE sync_state = 'sync';

-- Corroboration only, never the verdict on its own. `commit_lsn` is
-- pg_current_wal_lsn() read after the commit, so it sits at or past the commit
-- record's end; any WAL written in between (a checkpoint, the replication
-- manager's own statements) leaves a standby that genuinely flushed this
-- commit reporting a smaller flush_lsn. Treating that as "no standby held it"
-- would report a violation that did not happen.
SELECT 'SYNCACK', count(*)
FROM pg_stat_replication
WHERE sync_state = 'sync'
  AND flush_lsn >= :'commit_lsn'::pg_lsn;

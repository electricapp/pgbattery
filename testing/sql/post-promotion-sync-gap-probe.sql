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
--   PROMOTED|<clock>|<synchronous_standby_names>|<connected standbys>
--   NOTPROMOTED|<clock>|...        (this node stayed a standby; not a verdict)
--   ACKED|<clock>|<commit lsn>|<synchronous_standby_names at ack>
--   SYNCACK|<sync standbys whose flush_lsn covers the commit>
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
       (SELECT count(*) FROM pg_stat_replication);

-- Bound the commit only. A commit that waits this long has demonstrably
-- refused to acknowledge without a standby, which is the passing behaviour.
SET statement_timeout = '20s';

-- The commit under test.
INSERT INTO rw2_probe(tag) VALUES ('gap-write');

-- One LSN value, captured once and reused: re-reading it would drift past the
-- commit record and could under-report a standby that does hold the commit.
SELECT pg_current_wal_lsn() AS commit_lsn \gset

SELECT 'ACKED', clock_timestamp(), :'commit_lsn',
       current_setting('synchronous_standby_names');

-- Did a synchronous standby actually hold the commit at ack time? This is the
-- durability question: a non-zero count means the ack was backed by a standby
-- flush; zero means the primary acknowledged a write only it held.
SELECT 'SYNCACK', count(*)
FROM pg_stat_replication
WHERE sync_state = 'sync'
  AND flush_lsn >= :'commit_lsn'::pg_lsn;

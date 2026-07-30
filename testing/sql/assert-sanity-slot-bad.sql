-- Creates an inactive physical replication slot, the exact condition R1
-- forbids. Running replication-slot-no-leak-assert.sql after this MUST raise.
--
-- A slot nobody consumes pins restart_lsn, so the primary keeps every WAL
-- segment after it forever. The assertion passes trivially on a healthy
-- cluster, where every slot is active, so without this inversion it is
-- indistinguishable from an assertion that cannot fail.
--
-- Named distinctly from any slot pgbattery manages so cleanup cannot drop a
-- real one.
DO $$
BEGIN
    IF pg_is_in_recovery() THEN
        RAISE EXCEPTION 'must run on the primary; this node is in recovery';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = 'ci_sanity_leak_slot'
    ) THEN
        PERFORM pg_create_physical_replication_slot('ci_sanity_leak_slot', true);
    END IF;
END $$;

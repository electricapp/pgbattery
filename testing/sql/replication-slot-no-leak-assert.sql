-- Run with direct:true on the primary. Fails if any physical slot is inactive,
-- which would block WAL cleanup and cause unbounded disk growth.
DO $$
DECLARE
  stale TEXT;
BEGIN
  IF pg_is_in_recovery() THEN
    RETURN; -- standby: no slots to check here
  END IF;

  SELECT string_agg(slot_name || ' (retained=' ||
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) || ')', ', ')
  INTO stale
  FROM pg_replication_slots
  WHERE active = false AND slot_type = 'physical';

  IF stale IS NOT NULL THEN
    RAISE EXCEPTION 'Stale inactive physical replication slots on primary: %', stale;
  END IF;
END;
$$;

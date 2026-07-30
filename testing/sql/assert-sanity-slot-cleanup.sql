-- Drops only the slot assert-sanity-slot-bad.sql created. Never touches a slot
-- pgbattery manages: a cleanup that dropped a live standby's slot would break
-- replication for every case after it.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = 'ci_sanity_leak_slot'
    ) THEN
        PERFORM pg_drop_replication_slot('ci_sanity_leak_slot');
    END IF;
END $$;

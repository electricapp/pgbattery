-- End-to-end oracle for gateway connection migration: how many of the three
-- idle sessions held across the leader kill completed a write on their ORIGINAL
-- client connection afterwards.
--
-- The bound is 1..3, not exactly 3, because contract S2 permits severing: if
-- the new leader's PostgreSQL is not yet accepting when the gateway's
-- leader watch fires, the reconnect fails and that session is severed with
-- SQLSTATE 08006 (the client is then responsible for reconnecting). What is
-- NOT permitted is zero — that would mean the gateway dropped every idle
-- session instead of migrating any, i.e. the advertised migration path never
-- executed. More than three is impossible: only three sessions were held.
DO $$
DECLARE
    total_rows    INT;
    distinct_rows INT;
BEGIN
    SELECT COUNT(*), COUNT(DISTINCT session_id)
      INTO total_rows, distinct_rows
      FROM ci_gateway_migration;

    IF total_rows < 1 THEN
        RAISE EXCEPTION
            'gateway migration oracle: no held session wrote after the failover — every idle connection was dropped instead of migrated';
    END IF;
    IF total_rows > 3 THEN
        RAISE EXCEPTION
            'gateway migration oracle: % rows from 3 held sessions — a post-failover write was duplicated', total_rows;
    END IF;
    IF distinct_rows <> total_rows THEN
        RAISE EXCEPTION
            'gateway migration oracle: % rows but only % distinct sessions', total_rows, distinct_rows;
    END IF;
END $$;

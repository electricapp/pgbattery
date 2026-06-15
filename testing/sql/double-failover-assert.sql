-- Verify the pre-partition write survived and we can still write post-partition.
INSERT INTO double_failover_test (v) VALUES ('after-double-failover');
DO $$
BEGIN
  IF (SELECT count(*) FROM double_failover_test WHERE v IN ('before-second-failover', 'after-double-failover')) < 2 THEN
    RAISE EXCEPTION 'Expected both pre- and post-failover rows, got fewer';
  END IF;
END $$;

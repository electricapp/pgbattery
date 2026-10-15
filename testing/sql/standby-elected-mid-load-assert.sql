-- No client was acknowledged for the bulk load, so the new leader must not hold it.
DO $$
BEGIN
  IF to_regclass('h15_bulk') IS NOT NULL THEN
    RAISE EXCEPTION 'the unacknowledged bulk load is on the new leader';
  END IF;
END $$;

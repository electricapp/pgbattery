-- Each batch (worker_id) is a single INSERT...SELECT: it either committed fully
-- or not at all. Any count other than 0 or 1000 means partial write — lost atomicity.
DO $$
DECLARE
  w   INT;
  cnt BIGINT;
BEGIN
  FOR w IN SELECT DISTINCT worker_id FROM ci_concurrent_writes ORDER BY 1 LOOP
    SELECT count(*) INTO cnt FROM ci_concurrent_writes WHERE worker_id = w;
    IF cnt != 1000 THEN
      RAISE EXCEPTION
        'Partial write on worker %: % rows (expected 0 or 1000) — atomicity violated', w, cnt;
    END IF;
  END LOOP;
END;
$$;

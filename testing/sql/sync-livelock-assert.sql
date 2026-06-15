DO $$
BEGIN
  IF (SELECT count(*) FROM sync_livelock_test WHERE v = 'before-kill') < 1 THEN
    RAISE EXCEPTION 'Missing pre-kill row';
  END IF;
  IF (SELECT count(*) FROM sync_livelock_test WHERE v = 'after-kill') < 1 THEN
    RAISE EXCEPTION 'Missing post-kill row — writes were blocked when sync replica died';
  END IF;
END $$;

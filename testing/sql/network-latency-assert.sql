DO $$
DECLARE cnt INT;
BEGIN
    SELECT COUNT(*) INTO cnt FROM ci_network_latency;
    IF cnt <> 10 THEN
        RAISE EXCEPTION 'expected 10 rows under network latency, got %', cnt;
    END IF;
END $$;

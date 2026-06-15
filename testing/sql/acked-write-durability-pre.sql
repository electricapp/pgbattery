DO $$
BEGIN
    FOR i IN 1..30 LOOP
        INSERT INTO ci_ack_durability(client_id, op_id, payload)
        VALUES ('c1', i, 'pre')
        ON CONFLICT DO NOTHING;
    END LOOP;
END $$;

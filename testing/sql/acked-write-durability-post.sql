DO $$
BEGIN
    FOR i IN 31..60 LOOP
        INSERT INTO ci_ack_durability(client_id, op_id, payload)
        VALUES ('c1', i, 'post')
        ON CONFLICT DO NOTHING;
    END LOOP;
END $$;

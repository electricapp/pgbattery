CREATE TABLE IF NOT EXISTS ci_rogue_promote(
    id      SERIAL PRIMARY KEY,
    payload TEXT NOT NULL
);
TRUNCATE ci_rogue_promote;
DO $$
BEGIN
    FOR i IN 1..20 LOOP
        INSERT INTO ci_rogue_promote(payload) VALUES ('pre-promote-' || i);
    END LOOP;
END $$;

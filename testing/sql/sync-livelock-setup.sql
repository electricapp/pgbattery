CREATE TABLE IF NOT EXISTS sync_livelock_test (id serial PRIMARY KEY, v text, ts timestamptz DEFAULT now());
INSERT INTO sync_livelock_test (v) VALUES ('before-kill');

CREATE TABLE IF NOT EXISTS double_failover_test (id serial PRIMARY KEY, v text, ts timestamptz DEFAULT now());
INSERT INTO double_failover_test (v) VALUES ('before-second-failover');

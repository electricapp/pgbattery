-- Seeds ci_ack_durability with 60 rows of which 10 are duplicates, so the row
-- count is exactly right. Running acked-write-durability-assert.sql after this
-- MUST raise on the duplicate branch.
--
-- assert-sanity-acked seeds 5 distinct rows, which trips `total_rows <> 60` and
-- returns before the duplicate check is ever reached. So the branch that
-- catches a write applied twice -- the at-most-once half of what this assertion
-- guards, and the half R2's sync path depends on -- had never been observed
-- failing. Hitting the count exactly is the point: it forces execution past the
-- first branch.
CREATE TABLE IF NOT EXISTS ci_ack_durability(
    id        BIGSERIAL PRIMARY KEY,
    client_id INT  NOT NULL,
    op_id     INT  NOT NULL,
    payload   TEXT NOT NULL
);
TRUNCATE ci_ack_durability;
-- 50 distinct (client_id, op_id) pairs ...
INSERT INTO ci_ack_durability(client_id, op_id, payload)
    SELECT 1, generate_series(1, 50), 'dup-sanity';
-- ... plus 10 replays of pairs already present. 60 rows, 50 distinct.
INSERT INTO ci_ack_durability(client_id, op_id, payload)
    SELECT 1, generate_series(1, 10), 'dup-sanity-replay';

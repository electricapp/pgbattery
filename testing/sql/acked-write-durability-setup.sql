CREATE TABLE IF NOT EXISTS ci_ack_durability(
    client_id TEXT NOT NULL,
    op_id     INT  NOT NULL,
    payload   TEXT NOT NULL,
    PRIMARY KEY(client_id, op_id)
);
TRUNCATE ci_ack_durability;

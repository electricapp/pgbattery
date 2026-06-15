CREATE TABLE IF NOT EXISTS ci_tx_boundary(
    mode TEXT NOT NULL,
    seq  INT  NOT NULL,
    PRIMARY KEY(mode, seq)
);
TRUNCATE ci_tx_boundary;

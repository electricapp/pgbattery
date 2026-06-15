DO $$
DECLARE
    autocommit_rows INT;
    txn_rows        INT;
BEGIN
    SELECT COUNT(*) INTO autocommit_rows
        FROM ci_tx_boundary WHERE mode = 'autocommit';
    SELECT COUNT(*) INTO txn_rows
        FROM ci_tx_boundary WHERE mode = 'txn';

    IF autocommit_rows <> 1 THEN
        RAISE EXCEPTION 'autocommit row count mismatch: %', autocommit_rows;
    END IF;
    IF txn_rows NOT IN (0, 1) THEN
        RAISE EXCEPTION 'transaction row count invalid: %', txn_rows;
    END IF;
END $$;

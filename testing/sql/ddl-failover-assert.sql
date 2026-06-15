-- After a leader kill mid-DDL the schema must be fully committed or fully absent.
-- A half-created table (table without its PRIMARY KEY) is a partial DDL violation.
DO $$
DECLARE
    tbl_exists BOOL;
    has_pk     BOOL;
    orphan_idx BOOL;
BEGIN
    SELECT EXISTS(
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'public' AND tablename = 'ci_ddl_atomic'
    ) INTO tbl_exists;

    SELECT EXISTS(
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'public' AND indexname = 'ci_ddl_idx'
    ) INTO orphan_idx;

    IF tbl_exists THEN
        SELECT EXISTS(
            SELECT 1 FROM information_schema.table_constraints
            WHERE table_name = 'ci_ddl_atomic' AND constraint_type = 'PRIMARY KEY'
        ) INTO has_pk;
        IF NOT has_pk THEN
            RAISE EXCEPTION 'ci_ddl_atomic exists without its PRIMARY KEY: partial DDL committed';
        END IF;
    ELSE
        IF orphan_idx THEN
            RAISE EXCEPTION 'ci_ddl_idx exists without its table: orphaned catalog entry';
        END IF;
    END IF;
    -- Either fully committed or fully rolled back: both are valid outcomes.
END $$;

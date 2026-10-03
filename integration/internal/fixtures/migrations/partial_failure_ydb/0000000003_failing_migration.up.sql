-- This migration should fail after one valid statement. YDB runs each scheme
-- statement as a query of its own, outside any transaction, so the table it
-- creates stays and the revision records one query applied.
CREATE TABLE invalid_table (
    id Serial NOT NULL,
    PRIMARY KEY (id)
);
SELECT * FROM missing_partial_failure_dependency;

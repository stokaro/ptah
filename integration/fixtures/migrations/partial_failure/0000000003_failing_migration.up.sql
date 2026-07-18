-- This migration should fail after one valid statement. Transactional engines
-- roll the table back, but the metadata row still records failed progress.
CREATE TABLE invalid_table (
    id SERIAL PRIMARY KEY
);
SELECT * FROM missing_partial_failure_dependency;

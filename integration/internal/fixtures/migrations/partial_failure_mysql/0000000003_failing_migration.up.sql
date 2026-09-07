-- This migration intentionally fails after one successful statement. MySQL and
-- MariaDB auto-commit most DDL, so invalid_table remains half-applied and the
-- dirty metadata row blocks retries until repair.
CREATE TABLE invalid_table (
    id INT AUTO_INCREMENT PRIMARY KEY
);
UPDATE missing_partial_failure_dependency SET id = 1;

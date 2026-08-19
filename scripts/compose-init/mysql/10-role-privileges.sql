-- Privileges the role feature needs, granted to the account the integration
-- contour connects as.
--
-- MySQL and MariaDB keep roles in mysql.user and privileges in mysql.tables_priv
-- and mysql.db, and reading either needs SELECT on the mysql database -- a
-- privilege ordinary schema work does not need. Managing them needs more still:
-- CREATE USER creates and drops a role, and ROLE_ADMIN grants one.
--
-- This mirrors CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT on the ClickHouse service.
-- Without it the contour cannot exercise role support at all, and a suite that
-- skips the feature it is meant to cover reports the same green as one that
-- proves it (stokaro/ptah#1762).
--
-- The account keeps its ordinary shape otherwise. The degradation for an
-- account that holds none of this is covered by a test that creates one.
GRANT SELECT ON mysql.* TO 'ptah_user'@'%';
GRANT CREATE USER ON *.* TO 'ptah_user'@'%';
GRANT ALL PRIVILEGES ON ptah_test.* TO 'ptah_user'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

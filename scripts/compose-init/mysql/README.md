# Compose init scripts

The MySQL and MariaDB images run every `*.sql` in
`/docker-entrypoint-initdb.d` once, when the data directory is created.

They therefore take effect on a **fresh volume only**. A local checkout with an
existing `mysql_data` / `mariadb_data` volume keeps the privileges it was
created with; `docker compose down -v` recreates it. CI starts from nothing on
every run, so it always gets these.

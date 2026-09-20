#!/bin/sh
# Runs the zero-downtime page's own commands against a throwaway database.
#
# The page tells a reader what each command prints, so the printing is measured
# rather than read. What it checks is the page's claims in the page's order: a
# reader is refused behind a waiting ALTER, an apply gives up on its lock
# timeout, the linter names the rewrite for each unsafe operation, the rewritten
# directory has nothing left to report, and a concurrent build refuses a timeout
# it cannot honor.
#
#   docs/site/scripts/check-zero-downtime-changes.sh
#
# Against a remote Docker daemon the CLI has to reach the database itself, so
# name the host the fixture is reachable at:
#
#   PTAH_DOCKER_CONTEXT=<context> PTAH_FIXTURE_HOST=<host> \
#     docs/site/scripts/check-zero-downtime-changes.sh

set -eu

repo_dir=$(CDPATH='' cd -- "$(dirname -- "$0")/../../.." && pwd)
ptah=${PTAH_BIN:-$repo_dir/bin/ptah}
docker_context=${PTAH_DOCKER_CONTEXT:-default}
fixture_host=${PTAH_FIXTURE_HOST:-127.0.0.1}
# The page publishes 55434 so it collides neither with a PostgreSQL the reader
# already runs nor with the quick start's own fixture; the check uses the same.
port=${PTAH_ZERO_DOWNTIME_PORT:-55434}
container=ptah-zero-downtime-check-$$
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/ptah-zero-downtime.XXXXXX")

cleanup() {
	status=$?
	trap - 0 1 2 15
	docker --context "$docker_context" rm -f "$container" >/dev/null 2>&1 || true
	rm -rf -- "$work_dir"
	exit "$status"
}
trap cleanup 0 1 2 15

fail() {
	printf 'check-zero-downtime-changes: %s\n' "$1" >&2
	exit 1
}

# expect reads a command's output and fails unless it carries what the page
# tells the reader to expect. The page is the specification; this is the test.
expect() {
	expect_what=$1
	expect_output=$2
	printf '%s' "$expect_output" | grep -qF -- "$expect_what" ||
		fail "the page promises $(printf '%s' "$expect_what" | head -1), and the command printed:
$expect_output"
}

psql_in() {
	docker --context "$docker_context" exec "$container" psql -U ptah -d app -c "$1" 2>&1
}

[ -x "$ptah" ] || fail "no ptah binary at $ptah; run make build, or set PTAH_BIN"

printf 'check-zero-downtime-changes: starting the database\n'
docker --context "$docker_context" run -d --name "$container" \
	-e POSTGRES_USER=ptah -e POSTGRES_PASSWORD=ptah -e POSTGRES_DB=app \
	-p "${port}:5432" postgres:18-alpine >/dev/null

deadline=$(($(date +%s) + 120))
until docker --context "$docker_context" exec "$container" pg_isready -U ptah >/dev/null 2>&1; do
	[ "$(date +%s)" -lt "$deadline" ] || fail "the database did not accept connections within 120s"
	sleep 2
done

url="postgres://ptah:ptah@${fixture_host}:${port}/app?sslmode=disable"
cd "$work_dir"
mkdir migrations

printf 'check-zero-downtime-changes: the table the page seeds\n'
expect 'INSERT 0 50000' "$(psql_in "CREATE TABLE users (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL); INSERT INTO users (email) SELECT 'u' || g || '@example.com' FROM generate_series(1, 50000) g;")"

# The lock queue, which is the page's central claim: a reader cannot get in
# behind an ALTER that is itself waiting. The holder runs detached so the two
# statements below meet it rather than each other.
printf 'check-zero-downtime-changes: a reader refused behind a waiting ALTER\n'
docker --context "$docker_context" exec -d "$container" \
	psql -U ptah -d app -c 'BEGIN; SELECT count(*) FROM users; SELECT pg_sleep(30);'
sleep 2
expect 'canceling statement due to lock timeout' \
	"$(psql_in "SET lock_timeout = '1s'; ALTER TABLE users ADD COLUMN nickname TEXT;")"
docker --context "$docker_context" exec -d "$container" \
	psql -U ptah -d app -c 'ALTER TABLE users ADD COLUMN nickname TEXT;'
sleep 2
expect 'canceling statement due to lock timeout' \
	"$(psql_in "SET lock_timeout = '2s'; SELECT count(*) FROM users;")"

printf 'check-zero-downtime-changes: the linter names the rewrite\n'
cat >migrations/0000000001_unsafe.up.sql <<'SQL'
CREATE INDEX idx_users_email ON users (email);
ALTER TABLE users ALTER COLUMN email SET NOT NULL;
SQL
printf 'DROP INDEX idx_users_email;\n' >migrations/0000000001_unsafe.down.sql
lint_output=$("$ptah" migrations lint --dir ./migrations --dialect postgres 2>&1)
expect 'PG101: CREATE INDEX without CONCURRENTLY blocks writes to the table for the whole build' "$lint_output"
expect 'on a populated table use CREATE INDEX CONCURRENTLY outside a transaction' "$lint_output"
expect 'PG303: SET NOT NULL scans the whole table under an ACCESS EXCLUSIVE lock' "$lint_output"
expect 'backfill first, then add CHECK (col IS NOT NULL) NOT VALID, validate it under a weaker lock, and SET NOT NULL afterwards' "$lint_output"

printf 'check-zero-downtime-changes: the rewritten directory has nothing to report\n'
cat >migrations/0000000001_unsafe.up.sql <<'SQL'
-- +ptah no_transaction
CREATE INDEX CONCURRENTLY idx_users_email ON users (email);
SQL
cat >migrations/0000000001_unsafe.down.sql <<'SQL'
-- +ptah no_transaction
DROP INDEX CONCURRENTLY idx_users_email;
SQL
expect 'No lint findings.' "$("$ptah" migrations lint --dir ./migrations --dialect postgres 2>&1)"

printf 'check-zero-downtime-changes: a concurrent build refuses a timeout it cannot honor\n'
expect 'is marked no_transaction, so migration timeouts cannot be applied safely' \
	"$("$ptah" migrations up --db-url "$url" --migrations-dir ./migrations --lock-timeout 2s 2>&1 || true)"

printf 'check-zero-downtime-changes: an apply gives up on its lock timeout\n'
rm -rf migrations
mkdir migrations
cat >migrations/0000000001_bio.up.sql <<'SQL'
ALTER TABLE users ADD COLUMN bio TEXT;
SQL
printf 'ALTER TABLE users DROP COLUMN bio;\n' >migrations/0000000001_bio.down.sql
docker --context "$docker_context" exec -d "$container" \
	psql -U ptah -d app -c 'BEGIN; SELECT count(*) FROM users; SELECT pg_sleep(30);'
sleep 2
timeout_output=$("$ptah" migrations up --db-url "$url" --migrations-dir ./migrations --lock-timeout 2s 2>&1 || true)
expect 'canceling statement due to lock timeout (SQLSTATE 55P03)' "$timeout_output"
expect 'SQL: ALTER TABLE users ADD COLUMN bio TEXT' "$timeout_output"

printf 'check-zero-downtime-changes: OK (every claim the page makes)\n'

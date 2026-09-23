#!/bin/sh
# Runs the zero-downtime page's own commands against a throwaway database.
#
# The page tells a reader what each command prints, so the printing is measured
# rather than read. What it checks is the page's claims in the page's order: a
# reader is refused behind a waiting ALTER, an apply gives up on its lock
# timeout, the linter names the rewrite for each unsafe operation, the rewritten
# directory has nothing left to report, and the rewritten migration applies
# under the same lock timeout.
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
# behind an ALTER that is itself waiting. The page publishes every command
# below, in this order, including the two detached sessions -- a check that
# started one of them privately would be testing a page the reader cannot
# follow.
printf 'check-zero-downtime-changes: a reader refused behind a waiting ALTER\n'
docker --context "$docker_context" exec -d "$container" \
	psql -U ptah -d app -c "BEGIN; SELECT count(*) FROM users; SELECT pg_sleep(20);"
sleep 2
expect 'canceling statement due to lock timeout' \
	"$(psql_in "SET lock_timeout = '1s'; ALTER TABLE users ADD COLUMN nickname TEXT;")"
docker --context "$docker_context" exec -d "$container" \
	psql -U ptah -d app -c "ALTER TABLE users ADD COLUMN nickname TEXT;"
sleep 2
expect 'canceling statement due to lock timeout' \
	"$(psql_in "SET lock_timeout = '2s'; SELECT count(*) FROM users;")"

# The holder's twenty seconds have to run out before the next section holds the
# table again, or its migration queues behind this section's ALTER instead.
printf 'check-zero-downtime-changes: letting the first transaction finish\n'
sleep 20

printf 'check-zero-downtime-changes: an apply gives up on its lock timeout\n'
cat >migrations/0000000001_bio.up.sql <<'SQL'
ALTER TABLE users ADD COLUMN bio TEXT;
SQL
cat >migrations/0000000001_bio.down.sql <<'SQL'
ALTER TABLE users DROP COLUMN bio;
SQL
docker --context "$docker_context" exec -d "$container" \
	psql -U ptah -d app -c "BEGIN; SELECT count(*) FROM users; SELECT pg_sleep(20);"
sleep 2
timeout_output=$("$ptah" migrations up --db-url "$url" --migrations-dir ./migrations --lock-timeout 2s 2>&1 || true)
expect 'canceling statement due to lock timeout (SQLSTATE 55P03)' "$timeout_output"
expect 'SQL: ALTER TABLE users ADD COLUMN bio TEXT' "$timeout_output"

printf 'check-zero-downtime-changes: the failed run is dirty, and retrying clears it\n'
status_output=$("$ptah" migrations status --db-url "$url" --migrations-dir ./migrations 2>&1 || true)
expect 'Status: ❌ Dirty migration state detected' "$status_output"
expect 'Dirty Migration: version=1 state=failed direction=up applied=0/1' "$status_output"
# The page tells the reader to wait for the long transaction before retrying,
# and the retry cannot get its lock until this one is gone either.
sleep 20
expect 'Migrations completed successfully!' \
	"$("$ptah" migrations up --db-url "$url" --migrations-dir ./migrations --lock-timeout 2s --allow-dirty 2>&1)"
expect 'bio' "$(docker --context "$docker_context" exec "$container" psql -U ptah -d app -c '\d users' 2>&1)"

printf 'check-zero-downtime-changes: the linter names the rewrite\n'
cat >migrations/0000000002_unsafe.up.sql <<'SQL'
CREATE INDEX idx_users_email ON users (email);
ALTER TABLE users ALTER COLUMN email SET NOT NULL;
SQL
cat >migrations/0000000002_unsafe.down.sql <<'SQL'
DROP INDEX idx_users_email;
SQL
lint_output=$("$ptah" migrations lint --dir ./migrations --dialect postgres 2>&1)
expect 'PG106: DROP INDEX without CONCURRENTLY blocks writes' "$lint_output"
expect 'PG101: CREATE INDEX without CONCURRENTLY blocks writes to the table for the whole build' "$lint_output"
expect 'PG303: SET NOT NULL scans the whole table under an ACCESS EXCLUSIVE lock' "$lint_output"
expect 'backfill first, then add CHECK (col IS NOT NULL) NOT VALID, validate it under a weaker lock, and SET NOT NULL afterwards' "$lint_output"
expect '3 finding(s).' "$lint_output"

# The page names --fail-on any as the gate, so the value has to be one the
# command accepts. An invalid threshold would be a recommendation that exits on
# the flag rather than on a finding.
expect '3 finding(s).' "$("$ptah" migrations lint --dir ./migrations --dialect postgres --fail-on any 2>&1 || true)"

printf 'check-zero-downtime-changes: the rewritten directory has nothing to report\n'
cat >migrations/0000000002_unsafe.up.sql <<'SQL'
-- +ptah no_transaction
CREATE INDEX CONCURRENTLY idx_users_email ON users (email);
SQL
cat >migrations/0000000002_unsafe.down.sql <<'SQL'
-- +ptah no_transaction
DROP INDEX CONCURRENTLY idx_users_email;
SQL
expect 'No lint findings.' "$("$ptah" migrations lint --dir ./migrations --dialect postgres 2>&1)"

printf 'check-zero-downtime-changes: the rewritten migration runs under the lock timeout\n'
concurrent_output=$("$ptah" migrations up --db-url "$url" --migrations-dir ./migrations --lock-timeout 2s 2>&1)
expect 'Migrations completed successfully!' "$concurrent_output"
expect 'Database is now at version: 2' "$concurrent_output"

printf 'check-zero-downtime-changes: OK (every claim the page makes)\n'

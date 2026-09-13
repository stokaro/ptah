#!/bin/sh
# Runs the PostgreSQL quick start's own commands against a throwaway database.
#
# The page promises a result, so the result is measured rather than read. What
# it checks is the page's claims in the page's order: an empty database gets the
# table, the drift check agrees, asking for a column reports one finding, the
# plan is an ALTER, and applying it leaves the database matching again.
#
#   docs/site/scripts/check-postgresql-quick-start.sh
#
# Against a remote Docker daemon the CLI has to reach the database itself, so
# name the host the fixture is reachable at:
#
#   PTAH_DOCKER_CONTEXT=<context> PTAH_FIXTURE_HOST=<host> \
#     docs/site/scripts/check-postgresql-quick-start.sh

set -eu

repo_dir=$(CDPATH='' cd -- "$(dirname -- "$0")/../../.." && pwd)
ptah=${PTAH_BIN:-$repo_dir/bin/ptah}
docker_context=${PTAH_DOCKER_CONTEXT:-default}
fixture_host=${PTAH_FIXTURE_HOST:-127.0.0.1}
# The page publishes 55432 so it cannot collide with a PostgreSQL the reader
# already runs; the check uses the same port for the same reason.
port=${PTAH_QUICK_START_POSTGRES_PORT:-55432}
container=ptah-quick-start-check-$$
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/ptah-postgresql-quick-start.XXXXXX")

cleanup() {
	status=$?
	trap - 0 1 2 15
	docker --context "$docker_context" rm -f "$container" >/dev/null 2>&1 || true
	rm -rf -- "$work_dir"
	exit "$status"
}
trap cleanup 0 1 2 15

fail() {
	printf 'check-postgresql-quick-start: %s\n' "$1" >&2
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

[ -x "$ptah" ] || fail "no ptah binary at $ptah; run make build, or set PTAH_BIN"

printf 'check-postgresql-quick-start: starting the database\n'
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

cat >schema.sql <<'SQL'
CREATE TABLE users (
    id    BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL
);
SQL

printf 'check-postgresql-quick-start: step 3, the plan for an empty database\n'
expect 'CREATE TABLE "users" (' "$("$ptah" schema apply --schema-file schema.sql --db-url "$url" --dry-run 2>&1)"

printf 'check-postgresql-quick-start: step 4, applying it\n'
expect 'Schema apply completed successfully.' \
	"$("$ptah" schema apply --schema-file schema.sql --db-url "$url" --auto-approve 2>&1)"

printf 'check-postgresql-quick-start: step 5, the database agrees\n'
expect 'No schema drift detected.' "$("$ptah" schema drift --schema-file schema.sql --db-url "$url" 2>&1)"

cat >schema.sql <<'SQL'
CREATE TABLE users (
    id         BIGSERIAL PRIMARY KEY,
    email      TEXT NOT NULL,
    created_at TIMESTAMPTZ
);
SQL

printf 'check-postgresql-quick-start: step 6, asking for a column\n'
drift_output=$("$ptah" schema drift --schema-file schema.sql --db-url "$url" 2>&1 || true)
expect 'Schema drift detected (highest severity: warning).' "$drift_output"
expect '- columns_added: 1 (warning)' "$drift_output"
expect 'ALTER TABLE "users" ADD COLUMN "created_at" TIMESTAMPTZ;' \
	"$("$ptah" schema apply --schema-file schema.sql --db-url "$url" --dry-run 2>&1)"

printf 'check-postgresql-quick-start: step 7, applying the reviewed change\n'
expect 'Schema apply completed successfully.' \
	"$("$ptah" schema apply --schema-file schema.sql --db-url "$url" --auto-approve 2>&1)"
expect 'No schema drift detected.' "$("$ptah" schema drift --schema-file schema.sql --db-url "$url" 2>&1)"
expect 'created_at' \
	"$(docker --context "$docker_context" exec "$container" psql -U ptah -d app -c '\d users' 2>&1)"

printf 'check-postgresql-quick-start: OK (every step the page promises)\n'

#!/bin/sh

set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/ptah-examples.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT
trap 'exit 1' HUP INT TERM
example_gocache=${GOCACHE:-$work_dir/gocache}

run_go() {
	env GOCACHE="$example_gocache" go "$@"
}

cd "$repo_dir"

run_go run ./examples/annotation_parser ./examples/annotation_parser/models/example_entities.go >"$work_dir/annotation.txt"
if grep -qF 'Error ' "$work_dir/annotation.txt"; then
	echo 'check-examples: annotation_parser printed an error' >&2
	exit 1
fi
for expected in 'Found 4 tables' '=== POSTGRES ===' 'CREATE TABLE "users"' '=== MYSQL ===' '=== MARIADB ==='; do
	grep -qF "$expected" "$work_dir/annotation.txt" || {
		echo "check-examples: annotation_parser did not print $expected" >&2
		exit 1
	}
done

run_go run ./examples/extension_ignore >"$work_dir/extensions.txt"
for expected in \
	"1. Default Behavior (ignores 'plpgsql'):" \
	"2. Custom Ignore List (ignore 'adminpack' only):" \
	"3. Additional Ignored Extensions (default + 'adminpack'):" \
	'4. Manage All Extensions (no ignoring):' \
	'Extensions to remove: [{adminpack public true 2.1  [] []} {plpgsql pg_catalog true 1.0  [] []}]'; do
	grep -qF "$expected" "$work_dir/extensions.txt" || {
		echo "check-examples: extension_ignore did not print $expected" >&2
		exit 1
	}
done

run_go test ./examples/migrator -run TestExampleMigrations -count=1
run_go test ./examples/reusable_components -count=1

run_go run ./cmd/ptah viz --root-dir examples/viz/models --format mermaid --include-columns >"$work_dir/schema.mmd"
run_go run ./cmd/ptah viz --root-dir examples/viz/models --format dot --include-columns >"$work_dir/schema.dot"
cmp "$work_dir/schema.mmd" examples/viz/schema.mmd
cmp "$work_dir/schema.dot" examples/viz/schema.dot
grep -qF '<title>ptah_schema</title>' examples/viz/schema.svg
grep -qF '>organizations</text>' examples/viz/schema.svg

sh -n examples/orm-loaders/gorm/load-schema.sh
grep -qF 'ariga.io/atlas-provider-gorm@v0.6.1' examples/orm-loaders/gorm/load-schema.sh
grep -qF '.venv/bin/atlas-provider-sqlalchemy' examples/orm-loaders/sqlalchemy/ptah.yaml
grep -qF 'atlas-provider-sqlalchemy==0.4.1' examples/orm-loaders/sqlalchemy/requirements.txt
grep -qF 'SQLAlchemy==2.0.50' examples/orm-loaders/sqlalchemy/requirements.txt

(
	cd examples/orm-loaders/gorm
	env GOCACHE="$example_gocache" GOWORK=off go test -mod=readonly ./...
)

echo 'check-examples: OK (4 executed examples, viz artifacts, and 2 provider fixtures)'

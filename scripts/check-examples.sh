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
	'Extensions to remove: [adminpack plpgsql]'; do
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

# The minimal provider example. The commands are the README's own: the first
# bash block of its Run, Verify and Cleanup sections runs, in that order and in
# one shell as a reader runs them, in a copy of the example with this tree's
# ptah first on PATH, and each result the README promises is required. The
# tutorial page quotes the example's files, and
# docs/site/scripts/check-examples.mjs holds those excerpts to them.
provider_work="$work_dir/minimal-provider"
mkdir -p "$provider_work/bin" "$provider_work/example"
run_go build -o "$provider_work/bin/ptah" ./cmd/ptah
(cd examples/minimal-provider && git ls-files -z --cached --others --exclude-standard -- . | tar --null -T - -cf -) |
	tar -xf - -C "$provider_work/example"
: >"$provider_work/readme.sh"
for section in Run Verify Cleanup; do
	awk -v section="## $section" '
		/^## / { inside = ($0 == section) }
		inside && /^```bash$/ { capture = 1; next }
		capture && /^```$/ { exit }
		capture { print }
	' examples/minimal-provider/README.md >"$provider_work/$section.sh"
	if [ ! -s "$provider_work/$section.sh" ]; then
		echo "check-examples: examples/minimal-provider/README.md has no bash block under $section" >&2
		exit 1
	fi
	cat "$provider_work/$section.sh" >>"$provider_work/readme.sh"
	echo "echo '--- end of $section'" >>"$provider_work/readme.sh"
done
(
	cd "$provider_work/example"
	env PATH="$provider_work/bin:$PATH" GOCACHE="$example_gocache" GOWORK=off bash -eu "$provider_work/readme.sh"
) >"$provider_work/readme.txt" 2>&1 || {
	echo 'check-examples: the README blocks of examples/minimal-provider failed:' >&2
	cat "$provider_work/readme.txt" >&2
	exit 1
}
for expected in \
	'example.com/ptah-tutorial-provider 0.0.0-dev: compatible' \
	'CREATE TABLE "notes" ("id" INTEGER NOT NULL PRIMARY KEY, "body" TEXT);' \
	'Schema apply completed successfully.' \
	'--- end of Run' \
	'"outcome": "no-changes"' \
	'--- end of Cleanup'; do
	grep -qF -- "$expected" "$provider_work/readme.txt" || {
		echo "check-examples: the README blocks of examples/minimal-provider did not print $expected" >&2
		cat "$provider_work/readme.txt" >&2
		exit 1
	}
done
for leftover in ptah-tutorial-provider providers.json change.plan.json app.db; do
	if [ -e "$provider_work/example/$leftover" ]; then
		echo "check-examples: the Cleanup block of examples/minimal-provider/README.md left $leftover" >&2
		exit 1
	fi
done

# The staged rollout example, run the way the minimal provider example is: the
# first bash block of its Run, Verify and Cleanup sections, in one shell, in a
# copy of the example with this tree's ptah first on PATH. The rollout starts a
# ptah process per target, so the binary on PATH is also the one every target
# runs.
rollout_work="$work_dir/staged-rollout"
mkdir -p "$rollout_work/example"
(cd examples/staged-rollout && git ls-files -z --cached --others --exclude-standard -- . | tar --null -T - -cf -) |
	tar -xf - -C "$rollout_work/example"
: >"$rollout_work/readme.sh"
for section in Run Verify Cleanup; do
	awk -v section="## $section" '
		/^## / { inside = ($0 == section) }
		inside && /^```bash$/ { capture = 1; next }
		capture && /^```$/ { exit }
		capture { print }
	' examples/staged-rollout/README.md >"$rollout_work/$section.sh"
	if [ ! -s "$rollout_work/$section.sh" ]; then
		echo "check-examples: examples/staged-rollout/README.md has no bash block under $section" >&2
		exit 1
	fi
	cat "$rollout_work/$section.sh" >>"$rollout_work/readme.sh"
	echo "echo '--- end of $section'" >>"$rollout_work/readme.sh"
done
(
	cd "$rollout_work/example"
	env PATH="$provider_work/bin:$PATH" bash -eu "$rollout_work/readme.sh"
) >"$rollout_work/readme.txt" 2>&1 || {
	echo 'check-examples: the README blocks of examples/staged-rollout failed:' >&2
	cat "$rollout_work/readme.txt" >&2
	exit 1
}
for expected in \
	'Stage 3: wave-2' \
	'rollout exit status: 1' \
	'Stage wave-1: blocked by canary' \
	'Stage wave-1: succeeded: 3 of 3 targets succeeded, 2 required' \
	'Rollout succeeded' \
	'--- end of Run' \
	'Current Version: 2' \
	'--- end of Verify' \
	'--- end of Cleanup'; do
	grep -qF -- "$expected" "$rollout_work/readme.txt" || {
		echo "check-examples: the README blocks of examples/staged-rollout did not print $expected" >&2
		cat "$rollout_work/readme.txt" >&2
		exit 1
	}
done
awk '/^--- end of Run$/ { verify = 1; next } verify && /^--- end of Verify$/ { exit } verify && /^[0-9]+$/ { print }' \
	"$rollout_work/readme.txt" | grep -qx 9 || {
	echo 'check-examples: the staged rollout state did not record nine successes' >&2
	cat "$rollout_work/readme.txt" >&2
	exit 1
}
for leftover in databases approvals rollout-state.json rollout-state.json.lock; do
	if [ -e "$rollout_work/example/$leftover" ]; then
		echo "check-examples: the Cleanup block of examples/staged-rollout/README.md left $leftover" >&2
		exit 1
	fi
done

# The Kubernetes manifests need a cluster, so what is checked here is that every
# `ptah` argv in them names a verb this build registers and flags that verb
# accepts. That is the drift that actually happens: a flag renamed in the code
# and an example still naming the old spelling.
run_go test ./internal/cli/inference -run TestKubernetesExample -count=1

echo 'check-examples: OK (6 executed examples, viz artifacts, Kubernetes manifests, and 2 provider fixtures)'

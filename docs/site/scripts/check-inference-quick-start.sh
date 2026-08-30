#!/bin/sh

set -eu

repo_dir=$(CDPATH='' cd -- "$(dirname -- "$0")/../../.." && pwd)
generator="$repo_dir/docs/site/scripts/build-inference-quick-start.mjs"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/ptah-inference-quick-start.XXXXXX")
build_dir="$work_dir/build"
input_dir="$work_dir/input"
extract_dir="$work_dir/extracted"
results_dir="$work_dir/results"
project=${PTAH_INFERENCE_PROJECT:-ptah-docs-inference-$$}
docker_context=${PTAH_DOCKER_CONTEXT:-default}
fixture_host=${PTAH_FIXTURE_HOST:-127.0.0.1}
postgres_port=${PTAH_INFERENCE_POSTGRES_PORT:-55432}
embed_port=${PTAH_INFERENCE_EMBED_PORT:-58080}
fixture_dir=
cleanup_verified=0

compose() {
	PTAH_INFERENCE_POSTGRES_PORT=$postgres_port \
		PTAH_INFERENCE_EMBED_PORT=$embed_port \
		docker --context "$docker_context" compose -p "$project" -f "$fixture_dir/compose.yaml" "$@"
}

best_effort_cleanup() {
	if [ -n "$fixture_dir" ] && [ -f "$fixture_dir/compose.yaml" ]; then
		compose down -v --rmi local >/dev/null 2>&1 || true
	fi
}

cleanup_on_exit() {
	status=$?
	trap - 0 1 2 15
	if [ "$cleanup_verified" -ne 1 ]; then
		best_effort_cleanup
	fi
	rm -rf "$work_dir"
	exit "$status"
}
trap cleanup_on_exit 0 1 2 15

assert_no_project_resources() {
	containers=$(docker --context "$docker_context" ps -aq \
		--filter "label=com.docker.compose.project=$project")
	images=$(docker --context "$docker_context" image ls -q \
		--filter "label=com.docker.compose.project=$project")
	networks=$(docker --context "$docker_context" network ls -q \
		--filter "label=com.docker.compose.project=$project")
	volumes=$(docker --context "$docker_context" volume ls -q \
		--filter "label=com.docker.compose.project=$project")
	named_images=$(
		docker --context "$docker_context" image ls -q "${project}-postgres" 2>/dev/null
		docker --context "$docker_context" image ls -q "${project}-embeddings" 2>/dev/null
	)
	if [ -n "$containers$images$networks$volumes$named_images" ]; then
		echo "check-inference-quick-start: cleanup left Docker resources for $project" >&2
		[ -z "$containers" ] || echo "containers: $containers" >&2
		[ -z "$images$named_images" ] || echo "images: $images $named_images" >&2
		[ -z "$networks" ] || echo "networks: $networks" >&2
		[ -z "$volumes" ] || echo "volumes: $volumes" >&2
		return 1
	fi
}

mkdir -p "$build_dir" "$input_dir" "$extract_dir" "$results_dir"
node "$generator" --selftest
node "$generator"
node "$generator" --output-dir "$build_dir"

if [ -n "${PTAH_BIN:-}" ]; then
	cp "$PTAH_BIN" "$input_dir/ptah"
else
	(
		cd "$repo_dir"
		env GOCACHE="${GOCACHE:-$work_dir/gocache}" go build -o "$input_dir/ptah" ./cmd/ptah
	)
fi
cp "$build_dir/inference-quick-start.zip" "$input_dir/inference-quick-start.zip"

set -- "$input_dir"/*
if [ "$#" -ne 2 ]; then
	echo "check-inference-quick-start: isolated input has $# files instead of only ptah and the archive" >&2
	exit 1
fi

(
	cd "$extract_dir"
	unzip -q "$input_dir/inference-quick-start.zip"
)
fixture_dir="$extract_dir/inference-quick-start"
for required in README.md compose.yaml init.sql embed.py spec.yaml run.sh run.ps1 cleanup.sh cleanup.ps1; do
	if [ ! -f "$fixture_dir/$required" ]; then
		echo "check-inference-quick-start: archive omitted $required" >&2
		exit 1
	fi
done
if [ ! -x "$fixture_dir/run.sh" ] || [ ! -x "$fixture_dir/cleanup.sh" ]; then
	echo 'check-inference-quick-start: archive did not preserve executable Bash helpers' >&2
	exit 1
fi
if grep -R -n -E 'docs/site/fixtures/inference-quick-start|bin/ptah' "$fixture_dir"; then
	echo 'check-inference-quick-start: extracted fixture contains a repository-relative dependency' >&2
	exit 1
fi

export PATH="$input_dir:$PATH"
export PTAH_BIN=ptah
export PTAH_DOCKER_CONTEXT="$docker_context"
export PTAH_FIXTURE_HOST="$fixture_host"
export PTAH_INFERENCE_POSTGRES_PORT="$postgres_port"
export PTAH_INFERENCE_EMBED_PORT="$embed_port"
export PTAH_INFERENCE_PROJECT="$project"
export PTAH_DB_URL="postgres://ptah:ptah@$fixture_host:$postgres_port/ptah?sslmode=disable"
export PTAH_RUN_ID=quick-start

cd "$fixture_dir"
./run.sh up >"$results_dir/up.txt"
export PTAH_SPEC="$fixture_dir/.ptah-inference/spec.yaml"
grep -qF "endpoint: http://$fixture_host:$embed_port/v1" "$PTAH_SPEC"

ptah inference plan >"$results_dir/plan.txt"
for expected in \
	'source.estimated_rows = 3 (measured)' \
	'target.capability.vector_type = true (measured)' \
	'[backfill] embed 3 in-scope source rows' \
	'Consistency mode: outbox'; do
	grep -qF "$expected" "$results_dir/plan.txt" || {
		echo "check-inference-quick-start: plan omitted $expected" >&2
		exit 1
	}
done

ptah inference prepare
ptah inference backfill --batch-rows 10 >"$results_dir/backfill.txt"
grep -qF 'backfill finished: 3 scanned, 3 embedded, 0 skipped' "$results_dir/backfill.txt"
ptah inference catchup --batch-rows 10
ptah inference index
ptah inference verify >"$results_dir/verify.txt"
grep -qF '3 source rows, 3 target rows' "$results_dir/verify.txt"
grep -qF 'every deterministic layer passed' "$results_dir/verify.txt"
ptah inference status >"$results_dir/status.txt"

./run.sh rows >"$results_dir/rows.txt"
rows=$(compose exec -T postgres psql -U ptah -d ptah -Atc \
	"SELECT count(*) FROM docs WHERE embedding_generation IS NOT NULL AND embedding_state = 'upsert'")
if [ "$rows" != 3 ]; then
	echo "check-inference-quick-start: expected 3 candidate rows, got $rows" >&2
	exit 1
fi

plan_digest=$(./run.sh approval-digest 2>"$results_dir/cutover-refusal.txt")
if [ -z "$plan_digest" ]; then
	echo 'check-inference-quick-start: cutover refusal printed no plan digest' >&2
	exit 1
fi
grep -qF "plan $plan_digest" "$results_dir/cutover-refusal.txt"
export PTAH_APPROVE="$plan_digest"
ptah inference cutover --approver 'quick-start check' >"$results_dir/approved.txt"
grep -qF 'queries now read generation' "$results_dir/approved.txt"
grep -qF "(plan $plan_digest)" "$results_dir/approved.txt"

./run.sh pointer >"$results_dir/pointer.txt"
pointer_rows=$(compose exec -T postgres psql -U ptah -d ptah -Atc \
	"SELECT count(*) FROM ptah_embedding_pointer WHERE target_table = 'docs' AND active_generation <> ''")
if [ "$pointer_rows" != 1 ]; then
	echo "check-inference-quick-start: expected one active pointer, got $pointer_rows" >&2
	exit 1
fi

./cleanup.sh
if [ -e "$fixture_dir/.ptah-inference" ]; then
	echo 'check-inference-quick-start: cleanup left the runtime directory' >&2
	exit 1
fi
assert_no_project_resources

cd "$work_dir"
rm -rf "$extract_dir" "$input_dir" "$build_dir" "$results_dir"
for removed in "$extract_dir" "$input_dir" "$build_dir" "$results_dir"; do
	if [ -e "$removed" ]; then
		echo "check-inference-quick-start: cleanup left $removed" >&2
		exit 1
	fi
done
cleanup_verified=1
trap - 0 1 2 15
rm -rf "$work_dir"
if [ -e "$work_dir" ]; then
	echo "check-inference-quick-start: cleanup left $work_dir" >&2
	exit 1
fi

echo 'check-inference-quick-start: OK (isolated archive, lifecycle, approval, active pointer, and cleanup)'

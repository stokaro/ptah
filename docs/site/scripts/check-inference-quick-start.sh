#!/bin/sh

set -eu

repo_dir=$(CDPATH= cd -- "$(dirname -- "$0")/../../.." && pwd)
fixture_dir="$repo_dir/docs/site/fixtures/inference-quick-start"
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/ptah-inference-quick-start.XXXXXX")
project="ptah-docs-inference-$$"
docker_context=${PTAH_DOCKER_CONTEXT:-default}
fixture_host=${PTAH_FIXTURE_HOST:-127.0.0.1}
postgres_port=${PTAH_INFERENCE_POSTGRES_PORT:-55432}
embed_port=${PTAH_INFERENCE_EMBED_PORT:-58080}

compose() {
	docker --context "$docker_context" compose -p "$project" -f "$fixture_dir/compose.yaml" "$@"
}

cleanup() {
	compose down -v --rmi local >/dev/null 2>&1 || true
	rm -rf "$work_dir"
}
trap cleanup EXIT HUP INT TERM

if [ -n "${PTAH_BIN:-}" ]; then
	ptah_bin=$PTAH_BIN
else
	ptah_bin="$work_dir/ptah"
	env GOCACHE="${GOCACHE:-$work_dir/gocache}" go build -o "$ptah_bin" ./cmd/ptah
fi

sed "s#http://127.0.0.1:58080#http://$fixture_host:$embed_port#" \
	"$fixture_dir/spec.yaml" >"$work_dir/spec.yaml"
db_url="postgres://ptah:ptah@$fixture_host:$postgres_port/ptah?sslmode=disable"
run_id=quick-start

PTAH_INFERENCE_POSTGRES_PORT=$postgres_port \
	PTAH_INFERENCE_EMBED_PORT=$embed_port \
	compose up -d --build --wait

"$ptah_bin" inference plan --spec "$work_dir/spec.yaml" --db-url "$db_url" >"$work_dir/plan.txt"
for expected in \
	'source.estimated_rows = 3 (measured)' \
	'target.capability.vector_type = true (measured)' \
	'[backfill] embed 3 in-scope source rows' \
	'Consistency mode: outbox'; do
	grep -qF "$expected" "$work_dir/plan.txt" || {
		echo "check-inference-quick-start: plan omitted $expected" >&2
		exit 1
	}
done

"$ptah_bin" inference prepare --spec "$work_dir/spec.yaml" --db-url "$db_url" --run-id "$run_id"
"$ptah_bin" inference backfill --spec "$work_dir/spec.yaml" --db-url "$db_url" \
	--run-id "$run_id" --batch-rows 10 >"$work_dir/backfill.txt"
grep -qF 'backfill finished: 3 scanned, 3 embedded, 0 skipped' "$work_dir/backfill.txt"
"$ptah_bin" inference catchup --spec "$work_dir/spec.yaml" --db-url "$db_url" \
	--run-id "$run_id" --batch-rows 10
"$ptah_bin" inference index --spec "$work_dir/spec.yaml" --db-url "$db_url" --run-id "$run_id"
"$ptah_bin" inference verify --spec "$work_dir/spec.yaml" --db-url "$db_url" \
	--run-id "$run_id" >"$work_dir/verify.txt"
grep -qF '3 source rows, 3 target rows' "$work_dir/verify.txt"
grep -qF 'every deterministic layer passed' "$work_dir/verify.txt"

compose exec -T postgres psql -U ptah -d ptah -Atc \
	"SELECT count(*) FROM docs WHERE embedding_generation IS NOT NULL AND embedding_state = 'upsert'" \
	>"$work_dir/rows.txt"
grep -qx '3' "$work_dir/rows.txt"

if "$ptah_bin" inference cutover --spec "$work_dir/spec.yaml" --db-url "$db_url" \
	--run-id "$run_id" >"$work_dir/cutover.txt" 2>&1; then
	echo 'check-inference-quick-start: unapproved cutover unexpectedly succeeded' >&2
	exit 1
fi
plan_digest=$(sed -n 's/^plan //p' "$work_dir/cutover.txt" | head -1)
test -n "$plan_digest"
"$ptah_bin" inference cutover --spec "$work_dir/spec.yaml" --db-url "$db_url" \
	--run-id "$run_id" --approve "$plan_digest" --approver 'quick-start check' \
	>"$work_dir/approved.txt"
grep -qF "queries now read generation" "$work_dir/approved.txt"
grep -qF "(plan $plan_digest)" "$work_dir/approved.txt"

compose exec -T postgres psql -U ptah -d ptah -Atc \
	"SELECT count(*) FROM ptah_embedding_pointer WHERE target_table = 'docs' AND active_generation <> ''" \
	>"$work_dir/pointer.txt"
grep -qx '1' "$work_dir/pointer.txt"

echo 'check-inference-quick-start: OK (plan, candidate, verification, approval, and active pointer)'

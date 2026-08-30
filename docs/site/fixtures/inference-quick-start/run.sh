#!/bin/sh

set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
runtime_dir="$script_dir/.ptah-inference"
runtime_spec="$runtime_dir/spec.yaml"
docker_context=${PTAH_DOCKER_CONTEXT:-default}
fixture_host=${PTAH_FIXTURE_HOST:-127.0.0.1}
postgres_port=${PTAH_INFERENCE_POSTGRES_PORT:-55432}
embed_port=${PTAH_INFERENCE_EMBED_PORT:-58080}
project=${PTAH_INFERENCE_PROJECT:-ptah-inference-quick-start}
ptah_bin=${PTAH_BIN:-ptah}
db_url=${PTAH_DB_URL:-postgres://ptah:ptah@$fixture_host:$postgres_port/ptah?sslmode=disable}
run_id=${PTAH_RUN_ID:-quick-start}

compose() {
	PTAH_INFERENCE_POSTGRES_PORT=$postgres_port \
		PTAH_INFERENCE_EMBED_PORT=$embed_port \
		docker --context "$docker_context" compose -p "$project" -f "$script_dir/compose.yaml" "$@"
}

write_runtime_spec() {
	mkdir -p "$runtime_dir"
	temporary="$runtime_spec.tmp"
	found=0
	: >"$temporary"
	while IFS= read -r line || [ -n "$line" ]; do
		case "$line" in
		'  endpoint: __PTAH_INFERENCE_ENDPOINT__')
			printf '  endpoint: http://%s:%s/v1\n' "$fixture_host" "$embed_port" >>"$temporary"
			found=$((found + 1))
			;;
		*)
			printf '%s\n' "$line" >>"$temporary"
			;;
		esac
	done <"$script_dir/spec.yaml"
	if [ "$found" -ne 1 ]; then
		rm -f "$temporary"
		echo 'run.sh: spec.yaml must contain exactly one inference endpoint placeholder' >&2
		exit 1
	fi
	mv "$temporary" "$runtime_spec"
}

require_runtime() {
	if [ ! -f "$runtime_spec" ]; then
		echo 'run.sh: run "./run.sh up" before this command' >&2
		exit 1
	fi
}

start_services() {
	write_runtime_spec
	compose up -d --build --wait
	printf 'runtime specification: %s\n' "$runtime_spec"
	printf 'database URL: %s\n' "$db_url"
}

approval_digest() {
	require_runtime
	approval_output="$runtime_dir/cutover-refusal.txt"
	if "$ptah_bin" inference cutover \
		--spec "$runtime_spec" --db-url "$db_url" --run-id "$run_id" \
		>"$approval_output" 2>&1; then
		echo 'run.sh: unapproved cutover unexpectedly succeeded' >&2
		exit 1
	fi
	digest=
	while IFS= read -r line || [ -n "$line" ]; do
		printf '%s\n' "$line" >&2
		case "$line" in
		'plan '*)
			if [ -n "$digest" ]; then
				echo 'run.sh: cutover printed more than one plan digest' >&2
				exit 1
			fi
			digest=${line#plan }
			;;
		esac
	done <"$approval_output"
	rm -f "$approval_output"
	if [ -z "$digest" ]; then
		echo 'run.sh: cutover refusal printed no plan digest' >&2
		exit 1
	fi
	printf '%s\n' "$digest"
}

show_rows() {
	compose exec -T postgres psql -U ptah -d ptah -c \
		'SELECT id, embedding_generation, embedding_state FROM docs ORDER BY id;'
}

show_pointer() {
	compose exec -T postgres psql -U ptah -d ptah -c \
		'SELECT target_table, active_generation FROM ptah_embedding_pointer;'
}

cleanup() {
	compose down -v --rmi local
	rm -rf "$runtime_dir"
}

run_all() {
	start_services
	"$ptah_bin" inference plan --spec "$runtime_spec" --db-url "$db_url"
	"$ptah_bin" inference prepare --spec "$runtime_spec" --db-url "$db_url" --run-id "$run_id"
	"$ptah_bin" inference backfill --spec "$runtime_spec" --db-url "$db_url" --run-id "$run_id" --batch-rows 10
	"$ptah_bin" inference catchup --spec "$runtime_spec" --db-url "$db_url" --run-id "$run_id" --batch-rows 10
	"$ptah_bin" inference index --spec "$runtime_spec" --db-url "$db_url" --run-id "$run_id"
	"$ptah_bin" inference verify --spec "$runtime_spec" --db-url "$db_url" --run-id "$run_id"
	"$ptah_bin" inference status --spec "$runtime_spec" --db-url "$db_url" --run-id "$run_id"
	show_rows
	digest=$(approval_digest)
	"$ptah_bin" inference cutover --spec "$runtime_spec" --db-url "$db_url" --run-id "$run_id" \
		--approve "$digest" --approver 'quick-start helper'
	show_pointer
}

usage() {
	echo 'usage: ./run.sh {up|approval-digest|rows|pointer|cleanup|all}' >&2
}

command=${1:-}
case "$command" in
up)
	start_services
	;;
approval-digest)
	approval_digest
	;;
rows)
	show_rows
	;;
pointer)
	show_pointer
	;;
cleanup)
	cleanup
	;;
all)
	trap cleanup 0 1 2 15
	run_all
	cleanup
	trap - 0 1 2 15
	;;
*)
	usage
	exit 2
	;;
esac

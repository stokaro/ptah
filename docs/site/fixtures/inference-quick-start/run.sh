#!/bin/sh

set -eu

script_dir=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
runtime_dir="$script_dir/.ptah-inference"
runtime_spec="$runtime_dir/spec.yaml"
docker_context=${PTAH_DOCKER_CONTEXT:-default}
fixture_host=${PTAH_FIXTURE_HOST:-127.0.0.1}
# Empty by default, which publishes to a host port Docker chooses. The two
# variables stay for a caller that needs a fixed address -- a remote Docker
# context, where the ports have to be known to reach the host at all.
#
# They defaulted to 55432 and 58080, and both sit inside Linux's default
# ephemeral range (32768-60999): a run could lose the port to one of its own
# outbound connections and fail with `address already in use` on a machine
# where nothing else was listening (stokaro/ptah#2673).
postgres_port=${PTAH_INFERENCE_POSTGRES_PORT:-}
embed_port=${PTAH_INFERENCE_EMBED_PORT:-}
project=${PTAH_INFERENCE_PROJECT:-ptah-inference-quick-start}
ptah_bin=${PTAH_BIN:-ptah}
db_url=${PTAH_DB_URL:-}
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
	resolve_runtime
}

# published_port is the host port a service's container port ended up on.
#
# Asked of Docker rather than assumed, which is the whole point: a port this
# script picked could be taken between the picking and the binding, and one
# Docker assigned cannot be.
published_port() {
	compose port "$1" "$2" | sed 's/.*://'
}

start_services() {
	# Up FIRST, then the specification. It was the other way round, which is
	# why the ports had to be known in advance and therefore fixed
	# (stokaro/ptah#2673).
	compose up -d --build --wait
	postgres_port=$(published_port postgres 5432)
	embed_port=$(published_port embeddings 8080)
	if [ -z "$postgres_port" ] || [ -z "$embed_port" ]; then
		echo 'run.sh: docker did not report the published ports' >&2
		exit 1
	fi
	db_url="postgres://ptah:ptah@$fixture_host:$postgres_port/ptah?sslmode=disable"
	write_runtime_spec
	printf 'runtime specification: %s\n' "$runtime_spec"
	printf 'database URL: %s\n' "$db_url"
	printf 'embeddings endpoint: http://%s:%s/v1\n' "$fixture_host" "$embed_port"
}

# resolve_runtime recovers the addresses for a command that did not bring the
# stack up itself, since the ports are no longer knowable from the environment.
resolve_runtime() {
	[ -n "$postgres_port" ] || postgres_port=$(published_port postgres 5432)
	[ -n "$embed_port" ] || embed_port=$(published_port embeddings 8080)
	[ -n "$db_url" ] || db_url="postgres://ptah:ptah@$fixture_host:$postgres_port/ptah?sslmode=disable"
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

#!/usr/bin/env bash
# Proves capmatrix-probe-cell.sh restarts a dead server once, and only that.
#
# A retry that fires too eagerly hides a real capability disagreement, and one
# that never fires looks the same as a quiet week. So stubs stand in for docker
# and the driver, and each case requires its exit code and its docker writes.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
mkdir -p "$dir/bin" "$dir/runner"
printf '%s\n' --publish 1521:1521 image:tag >"$dir/runner/docker-args"

cat >"$dir/bin/docker" <<'STUB'
#!/usr/bin/env bash
case "$1" in
inspect)
	[ "$STUB_RUNNING" != none ] || exit 1
	echo "$STUB_RUNNING"
	;;
logs) echo "server log line" ;;
*) printf 'DOCKER %s\n' "$*" >>"$STUB_CALLS" ;;
esac
STUB
chmod +x "$dir/bin/docker"

failures=0
cases=0

# The driver answers from a list of exit codes, one per probe.
expect() {
	local name="$1" codes="$2" running="$3" want_code="$4" want_calls="$5"
	cases=$((cases + 1))
	: >"$dir/calls"
	printf '%s' "$codes" >"$dir/codes"
	local got=0
	(
		cd "$dir"
		mkdir -p results
		cat >"$dir/runner/capmatrix" <<STUB
#!/usr/bin/env bash
printf 'PROBE\n' >>"$dir/calls"
read -r first rest <"$dir/codes" || true
printf '%s' "\${rest:-}" >"$dir/codes"
echo "probe said \${first:-99}"
exit "\${first:-99}"
STUB
		chmod +x "$dir/runner/capmatrix"
		PATH="$dir/bin:$PATH" STUB_CALLS="$dir/calls" STUB_RUNNING="$running" \
			CELL_ID=oracle-21 RUNNER_TEMP="$dir/runner" \
			"$script_dir/capmatrix-probe-cell.sh" 2 4m "$dir/probe.log"
	) || got=$?
	[ "$got" = "$want_code" ] || {
		printf 'capmatrix-probe-cell-selftest: %s: exit %s, want %s\n' "$name" "$got" "$want_code" >&2
		failures=$((failures + 1))
	}
	local calls
	calls="$(tr '\n' ';' <"$dir/calls")"
	[ "$calls" = "$want_calls" ] || {
		printf 'capmatrix-probe-cell-selftest: %s: calls [%s], want [%s]\n' "$name" "$calls" "$want_calls" >&2
		failures=$((failures + 1))
	}
}

restart='DOCKER rm --force capability-probe-server;DOCKER run --detach --name capability-probe-server --publish 1521:1521 image:tag;'

expect 'the probe passes' '0' true 0 'PROBE;'
expect 'a passing probe keeps its answer after the server exits' '0' false 0 'PROBE;'
expect 'a live server answers wrongly' '1' true 1 'PROBE;'
expect 'a cell with no container fails' '1' none 1 'PROBE;'
expect 'a dead server starts once more and passes' '1 0' false 0 "PROBE;${restart}PROBE;"
expect 'a server that dies twice fails the cell' '1 1' false 1 "PROBE;${restart}PROBE;"

grep -qF 'server log line' "$dir/probe.log" || {
	echo 'capmatrix-probe-cell-selftest: the log leaves out the dead server output' >&2
	failures=$((failures + 1))
}
[ "$(grep -c 'probe said' "$dir/probe.log")" = 2 ] || {
	echo 'capmatrix-probe-cell-selftest: the log leaves out a probe' >&2
	failures=$((failures + 1))
}

if [ "$failures" -ne 0 ]; then
	printf 'capmatrix-probe-cell-selftest: %d failures in %d cases\n' "$failures" "$cases" >&2
	exit 1
fi
printf 'capmatrix-probe-cell-selftest: OK (%d cases)\n' "$cases"

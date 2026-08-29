#!/usr/bin/env bash
# Proves run-fuzz.sh passes only the shape it exists to pass.
#
# A wrapper that turns red into green is worth nothing unless it still goes red
# for a finding. Each case below drives it with a stub standing in for `go`, so
# the guard is measured rather than reasoned about.
set -uo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT

failures=0

# stub writes a fake `go` that prints a canned transcript and exits with a code.
stub() {
	local name="$1" code="$2" body="$3"
	local path="$work_dir/$name"
	{
		printf '#!/usr/bin/env bash\n'
		printf 'cat <<'"'"'TRANSCRIPT'"'"'\n%s\nTRANSCRIPT\n' "$body"
		printf 'exit %s\n' "$code"
	} >"$path"
	chmod +x "$path"
	printf '%s' "$path"
}

# expect drives the wrapper and checks whether it passed, not which non-zero
# code it chose: propagating go test's own status is deliberate, so a case that
# must fail asserts "not zero" rather than a number.
expect() {
	local name="$1" want_pass="$2" runner="$3"
	FUZZ_RUNNER="$runner" "$repo_root/scripts/run-fuzz.sh" FuzzThing 30s ./pkg >/dev/null 2>&1
	local got=$?
	local passed=false
	if [ "$got" -eq 0 ]; then
		passed=true
	fi
	if [ "$passed" != "$want_pass" ]; then
		printf 'run-fuzz-selftest: %s expected pass=%s, got exit %s\n' "$name" "$want_pass" "$got" >&2
		failures=$((failures + 1))
	fi
}

expect "a clean run passes" true "$(stub go_pass 0 'ok  	go.5x5.cz/ptah/pkg	30.4s')"

expect "a deadline with no finding passes" true "$(stub go_deadline 1 '--- FAIL: FuzzThing (30.31s)
    context deadline exceeded
FAIL')"

# The rows that must stay red.
expect "a written corpus entry fails" false "$(stub go_finding 1 '--- FAIL: FuzzThing (2.10s)
    Failing input written to testdata/fuzz/FuzzThing/abc123
FAIL')"

expect "a deadline alongside a real failure fails" false "$(stub go_both 1 'context deadline exceeded
--- FAIL: FuzzThing/seed#0 (0.00s)
    thing_test.go:12: round trip lost a token
FAIL')"

expect "a panic fails" false "$(stub go_panic 2 'panic: runtime error: index out of range
context deadline exceeded')"

expect "an unrelated failure fails" false "$(stub go_other 1 'build failed
FAIL')"

# The rows stokaro/ptah#2501 was filed for. Each is a real nonzero shape that
# names no panic, no race, no source location and no second --- FAIL, so the
# list of known-bad shapes this replaced passed every one of them.
expect "a runtime fatal beside the deadline fails" false "$(stub go_fatal 2 '--- FAIL: FuzzThing (30.31s)
    context deadline exceeded
fatal error: concurrent map writes
FAIL')"

expect "a deadlock beside the deadline fails" false "$(stub go_deadlock 2 '--- FAIL: FuzzThing (30.31s)
    context deadline exceeded
fatal error: all goroutines are asleep - deadlock!
FAIL')"

expect "a killed process beside the deadline fails" false "$(stub go_killed 1 '--- FAIL: FuzzThing (30.31s)
    context deadline exceeded
signal: killed
FAIL')"

# The controls. A deadline-only transcript still passes, and it still passes
# with the progress and package-summary lines a real run prints around it --
# without these, refusing everything would satisfy every row above.
expect "a deadline among a real run's own noise passes" true "$(stub go_deadline_noisy 1 'warning: starting with empty corpus
fuzz: elapsed: 0s, gathering baseline coverage: 0/12 completed
fuzz: elapsed: 3s, gathering baseline coverage: 12/12 completed, now fuzzing with 8 workers
fuzz: elapsed: 30s, execs: 122333 (4077/sec), new interesting: 3 (total: 15)
--- FAIL: FuzzThing (30.31s)
    context deadline exceeded
FAIL
FAIL	go.5x5.cz/ptah/pkg	30.4s')"

if [ "$failures" -ne 0 ]; then
	printf 'run-fuzz-selftest: %d case(s) failed\n' "$failures" >&2
	exit 1
fi
printf 'run-fuzz-selftest: the wrapper passes a bare deadline, with and without the progress lines a real run prints, and refuses a finding, a panic, a mixed failure, a runtime fatal, a deadlock and a killed process\n'

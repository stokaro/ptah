#!/usr/bin/env sh
# require-tests-ran.sh runs one package's tests matching a pattern and asserts
# that every test the pattern selects actually RAN.
#
# The workflow steps that call this used to carry, per protected test, a line of
# the shape
#
#	go test PKG -list '^TestExact$' | grep -q '^TestExact$'
#
# followed by one `go test -run` line covering the family. That pair answers a
# weaker question than the one CI needs, in three separate ways.
#
#  1. `go test -list` enumerates REGISTERED test functions. It does not run
#     them. A test that skips itself because its DSN is absent is listed exactly
#     like one that runs, so the guard passes on a job where nothing executed.
#     The failure being guarded against is an execution failure: this
#     repository's own workflow comment says a skip reads as a pass.
#  2. The names were maintained by hand, so the guard could only notice the
#     disappearance of a name somebody had remembered to add. A newly added live
#     test that never runs -- the defect this repository keeps rediscovering --
#     was invisible to it, and the lists had drifted to cover well under half of
#     the family they were protecting.
#  3. Reading `$?` after a pipeline reports the LAST stage, so the exit status
#     inspected belonged to `grep`, not to `go test`. That is the same defect
#     class the guard exists to close, which is why nothing below is piped: each
#     command writes a file and its status is read directly on the next line.
#
# What replaces them is one derived assertion, with no test names in YAML:
#
#	every test matching the pattern ran, and none skipped
#
# It is computed by counting what `-list` reports, running the same pattern with
# `-v`, and requiring the same number of top-level results with zero skips. An
# addition raises the listed count and must then show up as a result; a skip is
# refused outright; a pattern that has drifted away from every test selects
# nothing and is refused rather than silently passing.
#
# Renaming ONE test out of the family is the case those three do not cover: the
# listed count and the result count fall together, so they still agree. That is
# what `--declared-in` is for. It names the file the family lives in and
# requires every `func Test...` declared there to be selected by the pattern, so
# a test renamed out of the family -- or added to that file under a name the
# step does not run -- leaves the declared count above the listed count and
# fails. It is still derived: a path, not a list of names.
#
# usage:
#	sh scripts/require-tests-ran.sh --package ./migration/migrator \
#		--pattern '^TestRolledBackProgress_' --timeout 5m \
#		[--declared-in <file>]... [--tags integration]
set -eu

usage() {
	echo "usage: $0 --package <pkg> --pattern <regexp> --timeout <duration>" \
		"[--declared-in <file>]... [--tags <tags>]" >&2
}

package=""
pattern=""
timeout=""
tags=""
declared_in=""

while [ $# -gt 0 ]; do
	if [ $# -lt 2 ]; then
		usage
		exit 2
	fi
	case "$1" in
	--package) package="$2" ;;
	--pattern) pattern="$2" ;;
	--timeout) timeout="$2" ;;
	--tags) tags="$2" ;;
	--declared-in) declared_in="$declared_in $2" ;;
	*)
		usage
		exit 2
		;;
	esac
	shift 2
done

if [ -z "$package" ] || [ -z "$pattern" ] || [ -z "$timeout" ]; then
	usage
	exit 2
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# The build tags, if any, are carried in the positional parameters so that the
# empty case expands to nothing at all rather than to an empty argument.
if [ -n "$tags" ]; then
	set -- "-tags=$tags"
else
	set --
fi

work_dir="$(mktemp -d)"
trap 'rm -rf "$work_dir"' EXIT
list_log="$work_dir/list.log"
run_log="$work_dir/run.log"

list_status=0
go test "$@" "$package" -list "$pattern" >"$list_log" 2>&1 || list_status=$?
if [ "$list_status" -ne 0 ]; then
	cat "$list_log" >&2
	echo "require-tests-ran: could not enumerate $pattern in $package" >&2
	exit 1
fi

# `-list` prints one bare test name per matching entry, then the ordinary
# package result line ("ok  \tpkg\t1.2s" or "?   \tpkg\t[no test files]").
# Only the names begin with "Test", and every pattern this gate is used with is
# anchored at "^Test".
listed="$(grep -c '^Test' "$list_log" || true)"
if [ "$listed" -eq 0 ]; then
	cat "$list_log" >&2
	echo "require-tests-ran: pattern $pattern matches no test in $package" >&2
	echo "require-tests-ran: 'go test -run' would exit 0 printing 'no tests to run'" >&2
	exit 1
fi

# A test renamed out of the family shrinks the listed count and the result count
# together, so those two agreeing cannot see it. The family's home file can:
# every test declared there has to be one the pattern selects.
if [ -n "$declared_in" ]; then
	declared=0
	for file in $declared_in; do
		if [ ! -f "$file" ]; then
			echo "require-tests-ran: --declared-in file $file does not exist" >&2
			exit 1
		fi
		file_count="$(grep -c '^func Test' "$file" || true)"
		declared=$((declared + file_count))
	done
	if [ "$declared" -ne "$listed" ]; then
		echo "require-tests-ran: $declared tests are declared in$declared_in" >&2
		echo "require-tests-ran: but $pattern selects only $listed of them" >&2
		echo "require-tests-ran: a test declared there is not run by this step" >&2
		exit 1
	fi
fi

run_status=0
go test -v -count=1 "$@" "$package" -run "$pattern" -timeout "$timeout" >"$run_log" 2>&1 || run_status=$?
cat "$run_log"

# Top-level results are unindented; subtest results are indented by their depth,
# so anchoring at the start of the line counts each selected test exactly once.
passed="$(grep -c '^--- PASS: ' "$run_log" || true)"
failed="$(grep -c '^--- FAIL: ' "$run_log" || true)"
skipped="$(grep -c '^--- SKIP: ' "$run_log" || true)"
ran=$((passed + failed + skipped))

echo "require-tests-ran: $package $pattern: listed=$listed ran=$ran passed=$passed failed=$failed skipped=$skipped"

if [ "$run_status" -ne 0 ]; then
	echo "require-tests-ran: go test exited $run_status" >&2
	exit 1
fi
if [ "$skipped" -ne 0 ]; then
	grep '^--- SKIP: ' "$run_log" >&2 || true
	echo "require-tests-ran: $skipped of $listed tests skipped; a skip reads as a pass" >&2
	exit 1
fi
if [ "$ran" -ne "$listed" ]; then
	echo "require-tests-ran: $listed tests matched $pattern but $ran produced a result" >&2
	exit 1
fi

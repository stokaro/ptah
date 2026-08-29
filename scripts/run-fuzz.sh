#!/usr/bin/env bash
# Runs one fuzz target and tells a finding apart from the clock running out.
#
# `go test -fuzz` can exit non-zero with nothing but `context deadline
# exceeded` when -fuzztime elapses while workers are mid-execution. No input is
# minimized, nothing is written to testdata/fuzz, and the same command passes
# on the next run -- so the red check says only that the deadline landed badly.
#
# Measured on 2026-08-19: the CI job failed that way on one pull request while
# the identical command passed three times in a row locally, and nine of the
# ten runs around it were green.
#
# A real finding is loud and durable: `go test` prints `Failing input written
# to testdata/fuzz/<target>/<hash>` and leaves the file behind. That is the
# signal this keeps; a deadline with no such file is the non-event it is.
set -uo pipefail

if [ "$#" -lt 3 ]; then
	printf 'usage: %s <target> <fuzztime> <package>\n' "$0" >&2
	exit 2
fi

target="$1"
fuzztime="$2"
package="$3"

output="$("${FUZZ_RUNNER:-go}" test -run '^$' -fuzz="$target" -fuzztime="$fuzztime" -timeout=15m "$package" 2>&1)"
status=$?
printf '%s\n' "$output"

if [ "$status" -eq 0 ]; then
	exit 0
fi

if printf '%s' "$output" | grep -q 'Failing input written to'; then
	printf 'run-fuzz: %s found an input; the corpus entry above reproduces it\n' "$target" >&2
	exit "$status"
fi

if printf '%s' "$output" | grep -q 'context deadline exceeded'; then
	# Only when the deadline is the WHOLE story, and "the whole story" is
	# decided by recognizing every line rather than by looking for known bad
	# ones. A list of bad shapes passes whatever is not on it: `fatal error:
	# concurrent map writes` beside the deadline named no panic, no race, no
	# source location and no second --- FAIL, so it laundered into a green job
	# and took the only signal the run produced with it (stokaro/ptah#2501).
	#
	# So: a line this does not recognize keeps the run red. False red is the
	# safe direction here -- it costs a re-run, and the alternative costs a
	# finding.
	allowed="^--- FAIL: ${target} \([0-9.]+s\)$"
	allowed="${allowed}|^[[:space:]]+context deadline exceeded$"
	allowed="${allowed}|^FAIL$"
	# go's own package summary line, and nothing else beginning FAIL: a
	# `FAIL <pkg> [build failed]` carries no duration and stays unrecognized.
	allowed="${allowed}|^FAIL[[:space:]]+[^[:space:]]+[[:space:]]+[0-9.]+s$"
	allowed="${allowed}|^ok[[:space:]]"
	# The progress and setup lines a fuzz run prints while it works.
	allowed="${allowed}|^fuzz: elapsed:"
	allowed="${allowed}|^warning: starting with empty corpus$"

	unexplained="$(printf '%s\n' "$output" | grep -vE "$allowed" | grep -vE '^[[:space:]]*$' || true)"
	if [ -z "$unexplained" ]; then
		printf 'run-fuzz: %s hit the -fuzztime deadline mid-execution and found nothing; treating as a pass\n' "$target" >&2
		exit 0
	fi
	printf 'run-fuzz: %s printed lines a bare deadline does not explain:\n%s\n' "$target" "$unexplained" >&2
fi

printf 'run-fuzz: %s failed for a reason other than the deadline\n' "$target" >&2
exit "$status"

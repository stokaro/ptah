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
	# Only when the deadline is the WHOLE story. A run that also panicked, hit
	# the race detector, or failed an assertion is a failure like any other,
	# and the deadline line beside it must not launder it.
	#
	# The discriminators are what a real failure leaves and a bare deadline
	# never does: a source location, a panic, a race report, or a second
	# --- FAIL beneath the target's own.
	noise="$(printf '%s' "$output" | grep -cE 'panic:|DATA RACE|\.go:[0-9]+:' || true)"
	fails="$(printf '%s' "$output" | grep -cE '^[[:space:]]*--- FAIL' || true)"
	if [ "$noise" -eq 0 ] && [ "$fails" -le 1 ]; then
		printf 'run-fuzz: %s hit the -fuzztime deadline mid-execution and found nothing; treating as a pass\n' "$target" >&2
		exit 0
	fi
fi

printf 'run-fuzz: %s failed for a reason other than the deadline\n' "$target" >&2
exit "$status"

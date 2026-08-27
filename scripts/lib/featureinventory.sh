#!/usr/bin/env bash
# Shared runner for the five feature-coverage gates.
#
# Each gate is a set of Go tests in internal/featureinventory, selected with
# `go test -run`. That selection is the hazard this file exists for: `go test
# -run TestTypo` matches nothing, prints `ok`, and exits 0. A gate whose pattern
# stopped matching would report the same success it reports on a clean tree,
# which is the failure this repository names more often than any other.
#
# So the runner does not read the exit code alone. It counts the `func Test...`
# declarations the pattern selects, from the package source, and requires that
# many top-level tests to have actually reported PASS. The expected number is
# derived rather than written down, so a test added to a gate is covered by
# existing and a test deleted from one is a failure rather than a smaller run
# nobody noticed.

readonly FEATURE_INVENTORY_PKG="./internal/featureinventory"
readonly FEATURE_INVENTORY_DIR="internal/featureinventory"

# run_feature_inventory_gate <gate-name> <test-name-prefix> <what-it-holds>
run_feature_inventory_gate() {
	local gate="$1" prefix="$2" claim="$3"

	# `|| true` on the grep, because a pattern that matches nothing exits 1 and
	# `set -o pipefail` would end the script here -- with the right status and
	# no explanation. A gate that fails silently sends the reader looking for a
	# test failure that did not happen.
	local expected
	expected="$( (grep -hoE "^func ${prefix}[A-Za-z0-9_]*\(" "${FEATURE_INVENTORY_DIR}"/*_test.go || true) | sort -u | wc -l | tr -d ' ')"
	if [ "$expected" -eq 0 ]; then
		echo "${gate}: no test matches ${prefix}; this gate selects nothing and would pass on any tree" >&2
		return 1
	fi

	local output status=0
	output="$(go test "$FEATURE_INVENTORY_PKG" -run "^${prefix}" -count=1 -v 2>&1)" || status=$?

	local passed
	passed="$(printf '%s\n' "$output" | grep -c '^--- PASS: ' || true)"
	local failed
	failed="$(printf '%s\n' "$output" | grep -c '^--- FAIL: ' || true)"

	if [ "$status" -ne 0 ] || [ "$failed" -ne 0 ]; then
		echo "${gate}: ${claim}" >&2
		printf '%s\n' "$output" | sed 's/^/  /' >&2
		return 1
	fi
	if [ "$passed" -ne "$expected" ]; then
		echo "${gate}: ${passed} of ${expected} top-level tests matching ${prefix} reported PASS" >&2
		echo "  a gate that selects fewer tests than the package declares is reporting on less than it claims" >&2
		printf '%s\n' "$output" | sed 's/^/  /' >&2
		return 1
	fi

	echo "${gate}: OK (${passed} checks; ${claim})"
	return 0
}

# feature_inventory_usage prints the shared usage line and exits.
feature_inventory_usage() {
	echo "usage: $1 [--selftest]" >&2
	exit 2
}

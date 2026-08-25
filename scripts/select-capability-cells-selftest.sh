#!/usr/bin/env bash
# Proves select-capability-cells.sh selects what it claims, and refuses what it
# claims to refuse.
#
# The selector decides whether a run probes 26 cells or none, and the workflow
# it lives in cannot be exercised before it is on the default branch. Without
# this, the only evidence for any of the cases below is that somebody ran them
# once by hand -- and the prefix expression already went wrong once in exactly
# the way a single hand-run would not have noticed, matching every id against
# itself and selecting nothing.
#
# The fixture is synthetic on purpose. Reading the live matrix would make these
# counts change whenever a release line is added, and the properties being
# pinned are about the matching rule, not about which versions exist today.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
selector="$repo_root/scripts/select-capability-cells.sh"

# Two MySQL lines beside two MariaDB ones is the whole point of the fixture:
# `mysql` must not select MariaDB, and a rule that dropped the separator would
# select both while every other case below still passed.
cells='["postgres-18","postgres-17","mysql-9-7","mysql-8-4","mariadb-12-3","mariadb-11-8","oracle-21"]'

failures=0

# selected_for runs the selector and prints the selected array, or the word
# REFUSED when it exited non-zero.
selected_for() {
	local event="$1" request="$2" out
	if ! out="$("$selector" "$event" "$request" "$cells" 2>/dev/null)"; then
		echo REFUSED
		return 0
	fi
	printf '%s\n' "$out" | sed -n 's/^selected=//p'
}

expect() {
	local label="$1" event="$2" request="$3" want="$4" got
	got="$(selected_for "$event" "$request")"
	if [ "$got" != "$want" ]; then
		echo "select-capability-cells-selftest: ${label}: expected ${want}, got ${got}" >&2
		failures=$((failures + 1))
	fi
}

# A pull request probes nothing, and a schedule probes everything. These two are
# the policy the issue asked for; if they ever agree, the tier is either always
# on or always off and nothing below distinguishes the cases.
expect "a pull request selects no cells" pull_request "" "[]"
expect "a push selects no cells" push "" "[]"
expect "the nightly run selects every cell" schedule "" "$cells"
expect "dispatch with no argument selects every cell" workflow_dispatch "" "$cells"
expect "dispatch can ask for nothing" workflow_dispatch none "[]"

# The prefix rule. `mysql` selecting MariaDB too is the defect the separator
# exists to prevent, and it is the reason this fixture carries both.
expect "a dialect prefix selects that dialect's lines" workflow_dispatch postgres '["postgres-18","postgres-17"]'
expect "a dialect prefix stops at the separator" workflow_dispatch mysql '["mysql-9-7","mysql-8-4"]'
expect "an exact cell id selects only itself" workflow_dispatch postgres-17 '["postgres-17"]'
expect "a comma list selects the union, in matrix order" workflow_dispatch "mysql, oracle-21" '["mysql-9-7","mysql-8-4","oracle-21"]'
expect "surrounding space is not part of a name" workflow_dispatch "  postgres-18  " '["postgres-18"]'

# A request that names nothing is an error, not an empty run. `postgres-1` is
# the case that matters: it is a prefix of a real id, and a rule that matched on
# bare string prefixes would quietly probe postgres-18 instead of saying the
# request was wrong.
expect "a name the matrix does not have is refused" workflow_dispatch nosuch REFUSED
expect "a partial version is refused rather than rounded" workflow_dispatch postgres-1 REFUSED

# Every run says why, including the ones that selected nothing -- that sentence
# is the entire defense against an absent check reading like a passing one.
for event in pull_request push schedule workflow_dispatch; do
	reason="$("$selector" "$event" "" "$cells" | sed -n 's/^reason=//p')"
	if [ -z "$reason" ]; then
		echo "select-capability-cells-selftest: ${event} selected without saying why" >&2
		failures=$((failures + 1))
	fi
done

# The selector must not invent a matrix when it was handed none.
if "$selector" schedule "" "" >/dev/null 2>&1; then
	echo "select-capability-cells-selftest: an empty cell list was accepted" >&2
	failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
	echo "select-capability-cells-selftest: ${failures} case(s) failed" >&2
	exit 1
fi

echo "select-capability-cells-selftest: 16 cases checked, selection and refusal both observed"

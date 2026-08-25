#!/usr/bin/env bash
#
# Decide whether a diff can affect the compiled contours, and say so.
#
# The decision is stated as "run unless every changed file is provably inert",
# never as "run when a changed file matches a list". That direction is the whole
# point: a path nobody thought of runs the work, so a new directory added next
# month cannot silently turn a check off. `capability-matrix.yml` records the
# hazard this avoids -- a `paths:` filter at the workflow level makes the check
# ABSENT, and an absent check reads exactly like a passing one
# (stokaro/ptah#2185 point 5).
#
# This is deliberately not a `paths:` filter on the workflow. The job still
# runs, still reports, and names what it decided, so the check list is the same
# shape on every pull request.
#
# Usage:
#   scripts/ci-diff-scope.sh <base-ref-or-sha> <head-ref-or-sha>
#   scripts/ci-diff-scope.sh --selftest
#
# Writes `code=true|false` and `reason=<sentence>` to $GITHUB_OUTPUT when set,
# and always prints the decision.
set -euo pipefail

# Paths that cannot change what a compiled test does. Everything else runs the
# contour, including anything not listed here.
#
# `docs/**` is inert for the Go contours even though the docs workflow gates on
# it: that workflow has its own filter, and this one is about the integration
# and reference contours.
is_inert_path() {
	case "$1" in
	docs/*) return 0 ;;
	*.md) return 0 ;;
	.github/ISSUE_TEMPLATE/*) return 0 ;;
	.github/PULL_REQUEST_TEMPLATE*) return 0 ;;
	.github/FUNDING.yml) return 0 ;;
	LICENSE | LICENSE.*) return 0 ;;
	.gitignore | .gitattributes | .editorconfig) return 0 ;;
	*) return 1 ;;
	esac
}

# decide prints "true|false<TAB>reason" for a newline-separated file list.
decide() {
	local changed="$1"
	local file inert=0 total=0 first_active=""

	while IFS= read -r file; do
		[[ -z "$file" ]] && continue
		total=$((total + 1))
		if is_inert_path "$file"; then
			inert=$((inert + 1))
		elif [[ -z "$first_active" ]]; then
			first_active="$file"
		fi
	done <<<"$changed"

	# An empty diff runs the contour. A comparison that produced nothing is far
	# more likely a base that could not be resolved than a pull request that
	# changed no files, and the safe reading of "I could not tell" is to run.
	if [[ "$total" -eq 0 ]]; then
		printf 'true\tthe diff listed no files, so the contour runs rather than assuming it is unaffected\n'
		return
	fi
	if [[ -z "$first_active" ]]; then
		printf 'false\tall %d changed file(s) are documentation or repository metadata\n' "$inert"
		return
	fi
	printf 'true\t%s is not documentation or repository metadata\n' "$first_active"
}

selftest() {
	local failures=0
	assert() {
		local name="$1" want="$2" files="$3"
		local got
		got="$(decide "$files" | cut -f1)"
		if [[ "$got" != "$want" ]]; then
			printf 'ci-diff-scope selftest FAILED: %s (want %s, got %s)\n' "$name" "$want" "$got" >&2
			failures=$((failures + 1))
		fi
	}

	assert 'a Go file runs the contour' true 'internal/dbschema/postgres/reader.go'
	assert 'a docs-only change does not' false 'docs/migrations-import.md'
	assert 'a README anywhere does not' false $'README.md\ndocs/site/src/x.md'
	assert 'one Go file among docs still runs' true $'docs/a.md\ninternal/x.go'
	assert 'a workflow change runs the contour' true '.github/workflows/go-integration-tests.yml'
	assert 'a compose file runs the contour' true 'docker-compose.yaml'
	assert 'an unknown top-level directory runs the contour' true 'newthing/x.txt'
	assert 'an empty diff runs the contour' true ''
	assert 'go.mod runs the contour' true 'go.mod'
	assert 'an issue template does not' false '.github/ISSUE_TEMPLATE/bug.yml'
	assert 'a testdata SQL fixture runs the contour' true 'internal/x/testdata/a.sql'

	if [[ "$failures" -ne 0 ]]; then
		printf 'ci-diff-scope: %d selftest(s) failed\n' "$failures" >&2
		exit 1
	fi
	printf 'ci-diff-scope: OK (11 selftests, the inert set is closed and everything else runs)\n'
}

if [[ "${1:-}" == "--selftest" ]]; then
	selftest
	exit 0
fi

if [[ $# -ne 2 ]]; then
	printf 'usage: %s <base> <head>   (or --selftest)\n' "$0" >&2
	exit 2
fi

changed="$(git diff --name-only "$1" "$2")"
IFS=$'\t' read -r code reason < <(decide "$changed")

printf 'ci-diff-scope: code=%s (%s)\n' "$code" "$reason"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
	printf 'code=%s\n' "$code" >>"$GITHUB_OUTPUT"
	printf 'reason=%s\n' "$reason" >>"$GITHUB_OUTPUT"
fi

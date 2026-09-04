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
# plus the narrower `readme` and `public_api` scopes, and always prints the
# decision.
set -euo pipefail

# A Go module below the repository root is still compiled code even when its
# directory happens to live below docs/ or examples/. Discover those boundaries
# from the repository-wide module list instead of copying their paths here.
#
# Loading this list is deliberately strict. If module discovery stops producing
# the root module, classification itself is broken; failing the scope job is
# safer than teaching every guarded contour that the whole repository is inert.
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NESTED_GO_MODULES=()
load_nested_go_modules() {
	local module modules saw_root=false
	if ! modules="$("$script_dir/list-go-modules.sh")"; then
		printf '%s: could not discover the repository Go modules\n' "$0" >&2
		return 1
	fi
	while IFS= read -r module; do
		[[ -z "$module" ]] && continue
		if [[ "$module" == "." ]]; then
			saw_root=true
		else
			NESTED_GO_MODULES+=("$module")
		fi
	done <<<"$modules"
	if [[ "$saw_root" != true ]]; then
		printf '%s: module discovery omitted the repository root\n' "$0" >&2
		return 1
	fi
}

is_in_nested_go_module() {
	local file="$1" module
	if [[ "${#NESTED_GO_MODULES[@]}" -eq 0 ]]; then
		return 1
	fi
	for module in "${NESTED_GO_MODULES[@]}"; do
		if [[ "$file" == "$module" || "$file" == "$module/"* ]]; then
			return 0
		fi
	done
	return 1
}

load_nested_go_modules

# Paths that cannot change what a compiled test does. Everything else runs the
# contour, including anything not listed here.
#
# Ordinary documentation sources and the site package are inert for the Go
# contours even though the documentation workflow gates them. Go sources and
# module manifests below docs/ are not ordinary documentation, however, and a
# nested module is active in its entirety because its fixtures and generated
# inputs may be consumed by that module's tests.
is_inert_path() {
	if is_in_nested_go_module "$1"; then
		return 1
	fi
	case "$1" in
	*.go | *.s | *.S | *.c | *.cc | *.cpp | *.cxx | *.h | *.hh | *.hpp | *.hxx | *.m | *.mm | *.f | *.F | *.for | *.f90 | *.syso | *.swig | *.swigcxx) return 1 ;;
	go.mod | */go.mod | go.sum | */go.sum) return 1 ;;
	testdata/* | */testdata/*) return 1 ;;
	docs/architecture_boundaries.json) return 1 ;;
	docs/*) return 0 ;;
	*.md) return 0 ;;
	.github/ISSUE_TEMPLATE/*) return 0 ;;
	.github/PULL_REQUEST_TEMPLATE*) return 0 ;;
	.github/FUNDING.yml) return 0 ;;
	LICENSE | LICENSE.*) return 0 ;;
	.editorconfig) return 0 ;;
	*) return 1 ;;
	esac
}

# decide_paths prints "true|false<TAB>reason" for its file arguments. Runtime
# diff parsing uses an array so spaces and newlines in a tracked path cannot
# turn one path into several classifications.
decide_paths() {
	local file inert=0 total=0 first_active=""

	for file in "$@"; do
		[[ -z "$file" ]] && continue
		total=$((total + 1))
		if is_inert_path "$file"; then
			inert=$((inert + 1))
		elif [[ -z "$first_active" ]]; then
			first_active="$file"
		fi
	done

	# An empty diff runs the contour. A comparison that produced nothing is far
	# more likely a base that could not be resolved than a pull request that
	# changed no files, and the safe reading of "I could not tell" is to run.
	if [[ "$total" -eq 0 ]]; then
		printf 'true\tthe diff listed no files, so the contour runs rather than assuming it is unaffected\n'
		return
	fi
	if [[ -z "$first_active" ]]; then
		printf 'false\tall %d diff path(s) are documentation or repository metadata\n' "$inert"
		return
	fi
	printf 'true\t%q is not documentation or repository metadata\n' "$first_active"
}

# Kept for the existing self-test interface: decide accepts a newline-separated
# list. The production path below never uses it because Git paths may themselves
# contain newlines.
decide() {
	local changed="$1" file
	local -a paths=()
	while IFS= read -r file; do
		[[ -z "$file" ]] && continue
		paths+=("$file")
	done <<<"$changed"
	if [[ "${#paths[@]}" -eq 0 ]]; then
		decide_paths
	else
		decide_paths "${paths[@]}"
	fi
}

is_readme_scope_path() {
	case "$1" in
	README.md | scripts/check-readme-example*.sh) return 0 ;;
	*) return 1 ;;
	esac
}

is_public_api_scope_path() {
	case "$1" in
	docs/public_api.md | docs/public_api.snapshot | docs/public_api_approvals.txt | scripts/check-public-api*.sh | scripts/list-public-api-packages.sh) return 0 ;;
	*) return 1 ;;
	esac
}

# classify_paths appends the two narrow contract scopes to decide_paths'
# compatible code/reason pair. A rename is represented by two arguments, so a
# contract cannot disappear merely because its destination has another name.
classify_paths() {
	local decision code reason file readme=false public_api=false
	if [[ "$#" -eq 0 ]]; then
		decision="$(decide_paths)"
	else
		decision="$(decide_paths "$@")"
	fi
	IFS=$'\t' read -r code reason <<<"$decision"
	for file in "$@"; do
		if is_readme_scope_path "$file"; then
			readme=true
		fi
		if is_public_api_scope_path "$file"; then
			public_api=true
		fi
	done
	printf '%s\t%s\t%s\t%s\n' "$code" "$reason" "$readme" "$public_api"
}

# git diff --name-status -z emits STATUS,PATH records, except that renames and
# copies carry both the old and new path. Both sides of a rename matter: looking
# only at its inert destination would let moving compiled code into docs/ skip
# the contour that has to observe its removal.
DIFF_PATHS=()
parse_name_status_file() {
	local status path old_path new_path
	DIFF_PATHS=()
	while IFS= read -r -d '' status; do
		case "$status" in
		R[0-9]* | C[0-9]*)
			if ! IFS= read -r -d '' old_path || ! IFS= read -r -d '' new_path; then
				printf '%s: truncated %s record in git diff output\n' "$0" "$status" >&2
				return 1
			fi
			DIFF_PATHS+=("$old_path" "$new_path")
			;;
		A | B | D | M | T | U | X)
			if ! IFS= read -r -d '' path; then
				printf '%s: truncated %s record in git diff output\n' "$0" "$status" >&2
				return 1
			fi
			DIFF_PATHS+=("$path")
			;;
		*)
			printf '%s: unknown git diff status %q\n' "$0" "$status" >&2
			return 1
			;;
		esac
	done <"$1"
}

decide_name_status_file() {
	if ! parse_name_status_file "$1"; then
		# The narrow scopes also fail open. Current consumers OR them with code,
		# but keeping each output safe prevents a future direct consumer from
		# turning a parser regression into a skipped contract.
		printf 'true\tthe diff format was not recognized, so the contour runs rather than assuming it is unaffected\ttrue\ttrue\n'
		return
	fi
	if [[ "${#DIFF_PATHS[@]}" -eq 0 ]]; then
		classify_paths
	else
		classify_paths "${DIFF_PATHS[@]}"
	fi
}

decide_git_diff() {
	local base="$1" head="$2" diff_file
	diff_file="$(mktemp "${TMPDIR:-/tmp}/ptah-ci-diff-scope.XXXXXX")"
	# A pull request owns the commits after its merge base, not commits that
	# reached the base branch while the pull request was open. A two-dot diff
	# sees those base-only commits as deletions and needlessly turns a docs-only
	# pull request back into a full compiled run.
	if ! git diff --name-status -z --find-renames "$base...$head" -- >"$diff_file"; then
		rm -f "$diff_file"
		printf 'true\tthe diff could not be read, so the contour runs rather than assuming it is unaffected\ttrue\ttrue\n'
		return
	fi
	decide_name_status_file "$diff_file"
	rm -f "$diff_file"
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
	assert_name_status() {
		local name="$1" want="$2" records="$3" got result status_file
		status_file="$(mktemp "${TMPDIR:-/tmp}/ptah-ci-diff-scope-selftest.XXXXXX")"
		printf '%b' "$records" >"$status_file"
		result="$(decide_name_status_file "$status_file" 2>/dev/null)"
		rm -f "$status_file"
		got="${result%%$'\t'*}"
		if [[ "$got" != "$want" ]]; then
			printf 'ci-diff-scope selftest FAILED: %s (want %s, got %s)\n' "$name" "$want" "$got" >&2
			failures=$((failures + 1))
		fi
	}
	assert_scopes() {
		local name="$1" want_code="$2" want_readme="$3" want_public_api="$4" records="$5"
		local code reason readme public_api result status_file
		status_file="$(mktemp "${TMPDIR:-/tmp}/ptah-ci-diff-scope-selftest.XXXXXX")"
		printf '%b' "$records" >"$status_file"
		result="$(decide_name_status_file "$status_file" 2>/dev/null)"
		rm -f "$status_file"
		IFS=$'\t' read -r code reason readme public_api <<<"$result"
		if [[ "$code" != "$want_code" || "$readme" != "$want_readme" || "$public_api" != "$want_public_api" ]]; then
			printf 'ci-diff-scope selftest FAILED: %s (want %s/%s/%s, got %s/%s/%s)\n' \
				"$name" "$want_code" "$want_readme" "$want_public_api" "$code" "$readme" "$public_api" >&2
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
	assert '.gitattributes runs the contour because it controls checkout bytes' true '.gitattributes'
	assert '.gitignore runs the contour because freshness checks enumerate untracked files' true '.gitignore'
	assert 'the architecture ratchet baseline runs its checker' true 'docs/architecture_boundaries.json'
	assert 'a Markdown test fixture is compiled input, not prose' true 'internal/quickstart/testdata/pages/start/not-opted-in.md'
	assert 'a testdata SQL fixture runs the contour' true 'internal/x/testdata/a.sql'
	assert 'ordinary docs site package changes do not' false $'docs/site/package.json\ndocs/site/package-lock.json'
	assert 'Go code below docs still runs' true 'docs/docs.go'
	assert 'root-module Go fixtures below docs still run' true 'docs/site/fixtures/schema-ui/internal/models/schema.go'
	assert 'the docs nested module runs' true 'docs/site/fixtures/source-equivalence/models/schema.go'
	assert 'non-Go inputs in the docs nested module run' true 'docs/site/fixtures/source-equivalence/schema.sql'
	assert 'the example nested module runs in its entirety' true 'examples/orm-loaders/gorm/README.md'
	assert_name_status 'an active-to-inert rename still runs' true 'R100\0internal/x.go\0docs/x.md\0'
	assert_name_status 'an inert-to-active rename runs' true 'R100\0docs/x.md\0internal/x.go\0'
	assert_name_status 'an inert-to-inert rename stays inert' false 'R100\0docs/old.md\0docs/new.md\0'
	assert_name_status 'deleting a Markdown test fixture runs the contour' true 'D\0internal/atlasmigrateimport/testdata/ce-sums/goose/no-sql/README.md\0'
	assert_name_status 'an unknown diff record runs' true 'Q\0docs/x.md\0'
	assert_scopes 'README selects only the README contract' false true false 'M\0README.md\0'
	assert_scopes 'README checker changes select code and README' true true false 'M\0scripts/check-readme-example-selftest.sh\0'
	assert_scopes 'the public API ledger selects only its contract' false false true 'M\0docs/public_api.md\0'
	assert_scopes 'public API approvals select only their contract' false false true 'M\0docs/public_api_approvals.txt\0'
	assert_scopes 'the public API snapshot selects its contract' false false true 'M\0docs/public_api.snapshot\0'
	assert_scopes 'a public API checker selects code and its contract' true false true 'M\0scripts/check-public-api-released.sh\0'
	assert_scopes 'the public API package lister selects code and its contract' true false true 'M\0scripts/list-public-api-packages.sh\0'
	assert_scopes 'renaming the README still selects its contract' false true false 'R100\0README.md\0docs/old-readme.md\0'
	assert_scopes 'ordinary documentation selects neither narrow contract' false false false 'M\0docs/site/src/content/docs/index.mdx\0'
	assert_scopes 'an empty diff runs code without inventing a touched contract' true false false ''
	assert_scopes 'unrecognized diff data fails every scope open' true true true 'Q\0docs/x.md\0'

	# The base branch may advance after a pull request is opened. Only the
	# feature branch's docs commit belongs to that pull request; the later Go
	# commit on the base must not turn the docs-only classification into code.
	local topology_dir topology_trunk topology_head topology_result
	topology_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-ci-diff-scope-topology.XXXXXX")"
	git -C "$topology_dir" init --quiet
	topology_trunk="$(git -C "$topology_dir" symbolic-ref --short HEAD)"
	git -C "$topology_dir" config user.name 'CI scope selftest'
	git -C "$topology_dir" config user.email 'ci-scope@example.invalid'
	printf 'module example.invalid/scope\n\ngo 1.26\n' >"$topology_dir/go.mod"
	git -C "$topology_dir" add go.mod
	git -C "$topology_dir" -c commit.gpgsign=false commit --quiet -m base
	git -C "$topology_dir" switch --quiet -c docs-change
	mkdir -p "$topology_dir/docs"
	printf '# Documentation\n' >"$topology_dir/docs/change.md"
	git -C "$topology_dir" add docs/change.md
	git -C "$topology_dir" -c commit.gpgsign=false commit --quiet -m docs
	topology_head="$(git -C "$topology_dir" rev-parse HEAD)"
	git -C "$topology_dir" switch --quiet "$topology_trunk"
	printf 'package scope\n' >"$topology_dir/base_only.go"
	git -C "$topology_dir" add base_only.go
	git -C "$topology_dir" -c commit.gpgsign=false commit --quiet -m base-only-code
	topology_result="$(cd "$topology_dir" && decide_git_diff HEAD "$topology_head")"
	rm -rf "$topology_dir"
	if [[ "${topology_result%%$'\t'*}" != false ]]; then
		printf 'ci-diff-scope selftest FAILED: base-only commits changed pull-request scope (%s)\n' "$topology_result" >&2
		failures=$((failures + 1))
	fi

	if [[ "$failures" -ne 0 ]]; then
		printf 'ci-diff-scope: %d selftest(s) failed\n' "$failures" >&2
		exit 1
	fi
	printf 'ci-diff-scope: OK (38 selftests, merge-base topology, nested modules, contract scopes, and renames are covered)\n'
}

if [[ "${1:-}" == "--selftest" ]]; then
	selftest
	exit 0
fi

if [[ $# -ne 2 ]]; then
	printf 'usage: %s <base> <head>   (or --selftest)\n' "$0" >&2
	exit 2
fi

IFS=$'\t' read -r code reason readme public_api < <(decide_git_diff "$1" "$2")

printf 'ci-diff-scope: code=%s (%s)\n' "$code" "$reason"
printf 'ci-diff-scope: readme=%s, public_api=%s\n' "$readme" "$public_api"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
	{
		printf 'code=%s\n' "$code"
		printf 'reason=%s\n' "$reason"
		printf 'readme=%s\n' "$readme"
		printf 'public_api=%s\n' "$public_api"
	} >>"$GITHUB_OUTPUT"
fi

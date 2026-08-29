#!/usr/bin/env bash
# Proves check-go-module-lint-coverage.sh reports a module CI does not lint.
#
# What it exists to stop was measured: golangci-lint ran from the repository
# root and therefore linted only the root module, leaving a nested published
# module and examples/orm-loaders/gorm unlinted -- while nolintguard named each
# by hand in six places and qtlint discovered each by itself. Three tools, three
# answers to one question (stokaro/ptah#2509 moves the fixtures here).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-go-module-lint-coverage.sh"
lister="$repo_root/scripts/list-go-modules.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-module-lint.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

# write_repo builds a throwaway repository with the modules and the workflow the
# case is about. The modules are discovered from `git ls-files`, so each go.mod
# has to be tracked.
write_repo() {
	local workflow=$1
	shift
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/scripts" "$work_dir/repo/.github/workflows"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-go-module-lint-coverage.sh"
	cp "$lister" "$work_dir/repo/scripts/list-go-modules.sh"
	printf 'module example.com/root\n\ngo 1.26\n' >"$work_dir/repo/go.mod"
	for module in "$@"; do
		mkdir -p "$work_dir/repo/$module"
		printf 'module example.com/%s\n\ngo 1.26\n' "$module" >"$work_dir/repo/$module/go.mod"
	done
	printf '%s\n' "$workflow" >"$work_dir/repo/.github/workflows/go-lint.yml"
	git -C "$work_dir/repo" add -A
}

assert_rejected() {
	local name=$1 expected=$2
	if (cd "$work_dir/repo" && scripts/check-go-module-lint-coverage.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'go module lint coverage self-test: %s unexpectedly passed\n' "$name" >&2
		exit 1
	fi
	if ! grep -qF "$expected" "$work_dir/err"; then
		printf 'go module lint coverage self-test: %s failed for the wrong reason:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

assert_accepted() {
	local name=$1
	if ! (cd "$work_dir/repo" && scripts/check-go-module-lint-coverage.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'go module lint coverage self-test: %s was rejected:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

two_jobs_both_modules='jobs:
  lint:
    steps:
      - uses: golangci/golangci-lint-action@v9
      - uses: golangci/golangci-lint-action@v9
        with:
          working-directory: nested
  lint-windows:
    steps:
      - uses: golangci/golangci-lint-action@v9
      - uses: golangci/golangci-lint-action@v9
        with:
          working-directory: nested'

# A module no job visits: the state the gate was written for.
write_repo 'jobs:
  lint:
    steps:
      - uses: golangci/golangci-lint-action@v9' nested
assert_rejected 'a module no job lints' 'nested is linted by 0 of the 1 golangci-lint jobs'

# Dropped from ONE job. This is why the gate counts rather than searching: with
# `grep -q` the module stays green on the strength of the other job.
write_repo 'jobs:
  lint:
    steps:
      - uses: golangci/golangci-lint-action@v9
      - uses: golangci/golangci-lint-action@v9
        with:
          working-directory: nested
  lint-windows:
    steps:
      - uses: golangci/golangci-lint-action@v9' nested
assert_rejected 'a module dropped from one of two jobs' 'nested is linted by 1 of the 2 golangci-lint jobs'

# No unscoped invocation at all: nothing lints the root module.
write_repo 'jobs:
  lint:
    steps:
      - uses: golangci/golangci-lint-action@v9
        with:
          working-directory: nested' nested
assert_rejected 'no job linting the root module' 'runs golangci-lint in no job at all'

# The control. Without it, a gate refusing every workflow satisfies every row.
write_repo "$two_jobs_both_modules" nested
assert_accepted 'both jobs visiting both modules'

# And a second module has to appear in both jobs too, so the control is not
# passing on the strength of one module's arrangement.
write_repo "$two_jobs_both_modules" nested other
assert_rejected 'a second module visited by neither job' 'other is linted by 0 of the 2 golangci-lint jobs'

printf 'go module lint coverage self-test: an unlinted module, one dropped from a single job, and a workflow linting nothing are each reported\n'

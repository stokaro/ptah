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

# lint_step prints one golangci-lint step. `.` is the root module, which is the
# step that names no working-directory; tags is appended to the arguments.
lint_step() {
	local module=$1 tags=$2
	printf '      - uses: golangci/golangci-lint-action@v9\n'
	printf '        with:\n'
	printf '          args: --timeout=30m%s\n' "$tags"
	if [[ $module != "." ]]; then
		printf '          working-directory: %s\n' "$module"
	fi
}

# lint_steps prints a job that lints each module named in both contours.
lint_steps() {
	local job=$1
	shift
	printf '  %s:\n    steps:\n' "$job"
	for module in "$@"; do
		lint_step "$module" ""
		lint_step "$module" " --build-tags=integration"
	done
}

two_jobs_both_modules="jobs:
$(lint_steps lint . nested)
$(lint_steps lint-windows . nested)"

# A module no job visits: the state the gate was written for.
write_repo "jobs:
$(lint_steps lint .)" nested
assert_rejected 'a module no job lints' 'job lint in .github/workflows/go-lint.yml does not lint nested in the default contour'

# Dropped from ONE job. This is why the gate asks each job: searched over the
# file, the module stays green on the strength of the other job.
write_repo "jobs:
$(lint_steps lint . nested)
$(lint_steps lint-windows .)" nested
assert_rejected 'a module dropped from one of two jobs' 'job lint-windows in .github/workflows/go-lint.yml does not lint nested in the default contour'

# No golangci-lint step anywhere.
write_repo 'jobs:
  lint:
    steps:
      - run: make lint-qtlint' nested
assert_rejected 'a workflow running golangci-lint nowhere' 'runs golangci-lint in no job at all'

# A job linting only the nested module: nothing in it lints the root.
write_repo "jobs:
$(lint_steps lint nested)" nested
assert_rejected 'a job that never lints the root module' 'job lint in .github/workflows/go-lint.yml does not lint the root module in the default contour'

# The control. Without it, a gate refusing every workflow satisfies every row.
write_repo "$two_jobs_both_modules" nested
assert_accepted 'both jobs visiting both modules in both contours'

# The build tag spelled with a space is the same contour.
write_repo "jobs:
$(lint_steps lint . nested | sed 's/--build-tags=/--build-tags /')" nested
assert_accepted 'the build tag spelled with a space'

# And a second module has to appear in both jobs too, so the control is not
# passing on the strength of one module's arrangement.
write_repo "$two_jobs_both_modules" nested other
assert_rejected 'a second module visited by neither job' 'job lint in .github/workflows/go-lint.yml does not lint other in the default contour'

# The root module never linted with the integration tag: every golangci-lint
# step passes no tags, which is how 84 findings in integration/ went unread
# (stokaro/ptah#3882).
write_repo "$(printf '%s\n' "$two_jobs_both_modules" | grep -v 'build-tags')" nested
assert_rejected 'no step linting the integration contour' 'job lint in .github/workflows/go-lint.yml does not lint the root module in the integration contour'

# The same step twice: the step count is right and the contour is not. A count
# over the file cannot see this; asking each step for its contour does.
write_repo "$(printf '%s\n' "$two_jobs_both_modules" | sed 's/ --build-tags=integration//')" nested
assert_rejected 'the default contour linted twice' 'job lint in .github/workflows/go-lint.yml does not lint the root module in the integration contour'

# A tag named only in a comment is not a tag the step passes.
write_repo "$(printf '%s\n' "$two_jobs_both_modules" | sed 's/ --build-tags=integration/ # --build-tags=integration/')" nested
assert_rejected 'a build tag named in a comment' 'job lint in .github/workflows/go-lint.yml does not lint the root module in the integration contour'

# A nested module missing only its integration step in one job.
write_repo "jobs:
$(lint_steps lint . nested)
  lint-windows:
    steps:
$(lint_step . '')
$(lint_step . ' --build-tags=integration')
$(lint_step nested '')" nested
assert_rejected 'a nested module missing one contour in one job' 'job lint-windows in .github/workflows/go-lint.yml does not lint nested in the integration contour'

printf 'go module lint coverage self-test: an unlinted module, one dropped from a single job, a workflow linting nothing, and a missing integration contour are each reported\n'

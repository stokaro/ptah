#!/usr/bin/env bash
# Proves check-go-module-lint-coverage.sh reports a module or a build-tag
# contour CI does not lint.
#
# What it exists to stop was measured: golangci-lint ran from the repository
# root and therefore linted only the root module, leaving a nested published
# module and examples/orm-loaders/gorm unlinted -- while nolintguard named each
# by hand in six places and qtlint discovered each by itself. Three tools, three
# answers to one question (stokaro/ptah#2509 moves the fixtures here). The same
# holds for build tags: a step without them reads no integration test and no
# observability file (stokaro/ptah#3882, stokaro/ptah#3895).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-go-module-lint-coverage.sh"
lister="$repo_root/scripts/list-go-modules.sh"
tag_lister="$repo_root/scripts/list-build-tags.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-module-lint.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

# The tagged contour every accepted fixture lints: each opt-in tag the default
# tagged files below use.
tags=integration,observability

# tagged_files is the Go files write_repo writes, one "path|constraint" per
# line. The default is one file per opt-in tag; a case about tags sets it
# before write_repo and restores it after.
default_tagged_files='integration/a_test.go|integration
internal/obs/otel.go|observability'
tagged_files=$default_tagged_files

# security_workflow is the go-security.yml write_repo writes. By default it is a
# gosec job linting the root module in both contours; a case about that file
# sets it before write_repo and restores it after.
default_security_workflow="jobs:
  gosec:
    steps:
      - uses: golangci/golangci-lint-action@v9
        with:
          args: --enable-only=gosec --output.sarif.path=gosec.sarif
      - uses: golangci/golangci-lint-action@v9
        with:
          args: --enable-only=gosec --build-tags=$tags --output.sarif.path=gosec-integration.sarif"
security_workflow=$default_security_workflow

# write_repo builds a throwaway repository with the modules and the go-lint.yml
# the case is about. Modules and tags are discovered from `git ls-files`, so
# every go.mod and Go file has to be tracked.
write_repo() {
	local workflow=$1 path constraint
	shift
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/scripts" "$work_dir/repo/.github/workflows"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-go-module-lint-coverage.sh"
	cp "$lister" "$work_dir/repo/scripts/list-go-modules.sh"
	cp "$tag_lister" "$work_dir/repo/scripts/list-build-tags.sh"
	printf 'module example.com/root\n\ngo 1.26\n' >"$work_dir/repo/go.mod"
	for module in "$@"; do
		mkdir -p "$work_dir/repo/$module"
		printf 'module example.com/%s\n\ngo 1.26\n' "$module" >"$work_dir/repo/$module/go.mod"
	done
	while IFS='|' read -r path constraint; do
		[[ -n $path ]] || continue
		mkdir -p "$work_dir/repo/$(dirname "$path")"
		printf '//go:build %s\n\npackage fixture\n' "$constraint" >"$work_dir/repo/$path"
	done <<<"$tagged_files"
	printf '%s\n' "$workflow" >"$work_dir/repo/.github/workflows/go-lint.yml"
	printf '%s\n' "$security_workflow" >"$work_dir/repo/.github/workflows/go-security.yml"
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
# step that names no working-directory; extra is appended to the arguments.
lint_step() {
	local module=$1 extra=$2
	printf '      - uses: golangci/golangci-lint-action@v9\n'
	printf '        with:\n'
	printf '          args: --timeout=30m%s\n' "$extra"
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
		lint_step "$module" " --build-tags=$tags"
	done
}

two_jobs_both_modules="jobs:
$(lint_steps lint . nested)
$(lint_steps lint-windows . nested)"

# A module no job visits: the state the gate was written for.
write_repo "jobs:
$(lint_steps lint .)" nested
assert_rejected 'a module no job lints' 'job lint in .github/workflows/go-lint.yml does not lint nested without build tags'

# Dropped from ONE job. This is why the gate asks each job: searched over the
# file, the module stays green on the strength of the other job.
write_repo "jobs:
$(lint_steps lint . nested)
$(lint_steps lint-windows .)" nested
assert_rejected 'a module dropped from one of two jobs' 'job lint-windows in .github/workflows/go-lint.yml does not lint nested without build tags'

# No golangci-lint step anywhere.
write_repo 'jobs:
  lint:
    steps:
      - run: make lint-qtlint' nested
assert_rejected 'a workflow running golangci-lint nowhere' 'runs golangci-lint in no job at all'

# A job linting only the nested module: nothing in it lints the root.
write_repo "jobs:
$(lint_steps lint nested)" nested
assert_rejected 'a job that never lints the root module' 'job lint in .github/workflows/go-lint.yml does not lint the root module without build tags'

# The control. Without it, a gate refusing every workflow satisfies every row.
write_repo "$two_jobs_both_modules" nested
assert_accepted 'both jobs visiting both modules in both contours'

# The build tag spelled with a space is the same contour.
write_repo "jobs:
$(lint_steps lint . nested | sed 's/--build-tags=/--build-tags /')" nested
assert_accepted 'the build tag spelled with a space'

# The tags are a set: another order is the same contour.
write_repo "$(printf '%s\n' "$two_jobs_both_modules" | sed 's/--build-tags=integration,observability/--build-tags=observability,integration/')" nested
assert_accepted 'the tags in another order'

# And a second module has to appear in both jobs too, so the control is not
# passing on the strength of one module's arrangement.
write_repo "$two_jobs_both_modules" nested other
assert_rejected 'a second module visited by neither job' 'job lint in .github/workflows/go-lint.yml does not lint other without build tags'

# The root module never linted with tags: every golangci-lint step passes none,
# so no tagged file is read (stokaro/ptah#3882).
write_repo "$(printf '%s\n' "$two_jobs_both_modules" | grep -v 'build-tags')" nested
assert_rejected 'no step linting the tagged contour' "job lint in .github/workflows/go-lint.yml does not lint the root module with --build-tags=$tags"

# The same step twice: the step count is right and the contour is not. A count
# over the file cannot see this; asking each step for its contour does.
write_repo "$(printf '%s\n' "$two_jobs_both_modules" | sed "s/ --build-tags=$tags//")" nested
assert_rejected 'the default contour linted twice' "job lint in .github/workflows/go-lint.yml does not lint the root module with --build-tags=$tags"

# A tag named only in a comment is not a tag the step passes.
write_repo "$(printf '%s\n' "$two_jobs_both_modules" | sed "s/ --build-tags=$tags/ # --build-tags=$tags/")" nested
assert_rejected 'a build tag named in a comment' "job lint in .github/workflows/go-lint.yml does not lint the root module with --build-tags=$tags"

# A tag the tree uses and no step passes: the observability tag, which nothing
# linted (stokaro/ptah#3895).
write_repo "$(printf '%s\n' "$two_jobs_both_modules" | sed "s/--build-tags=$tags/--build-tags=integration/")" nested
assert_rejected 'a tag the tree uses missing from every step' "job lint in .github/workflows/go-lint.yml does not lint the root module with --build-tags=$tags"

# A tag a file gains later fails until the workflow passes it too. This is what
# the discovery buys over a written tag list.
tagged_files="$default_tagged_files
internal/newtag/new.go|newtag"
write_repo "$two_jobs_both_modules" nested
assert_rejected 'a new tag in the tree' 'job lint in .github/workflows/go-lint.yml does not lint the root module with --build-tags=integration,newtag,observability'
tagged_files=$default_tagged_files

# Platform names are not opt-in tags, and a file under testdata/ is never
# built, so neither changes the tagged contour.
tagged_files="$default_tagged_files
internal/platform/unix.go|linux && !windows
internal/runner/testdata/fixture/f.go|testcontour_fixture"
write_repo "$two_jobs_both_modules" nested
assert_accepted 'platform tags and a testdata tag'
tagged_files=$default_tagged_files

# A file that needs one opt-in tag and excludes another is in neither contour,
# so the tag lister refuses it rather than let both runs miss it.
tagged_files="$default_tagged_files
internal/both/both.go|integration && !observability"
write_repo "$two_jobs_both_modules" nested
assert_rejected 'a constraint pairing two opt-in tags with a negation' 'names two opt-in tags and negates one'
tagged_files=$default_tagged_files

# A tree with no opt-in tag has no tagged contour to require, and an empty list
# is how a broken discovery would look, so it is refused.
tagged_files=''
write_repo "$two_jobs_both_modules" nested
assert_rejected 'no opt-in tag in the tree' 'no opt-in build tag discovered'
tagged_files=$default_tagged_files

# A nested module missing only its tagged step in one job.
write_repo "jobs:
$(lint_steps lint . nested)
  lint-windows:
    steps:
$(lint_step . '')
$(lint_step . " --build-tags=$tags")
$(lint_step nested '')" nested
assert_rejected 'a nested module missing one contour in one job' "job lint-windows in .github/workflows/go-lint.yml does not lint nested with --build-tags=$tags"

# The gosec code-scanning job running without tags: the state stokaro/ptah#3895
# found, where code scanning showed no tagged file. Only the root is asked of
# it, so the nested module here is not a gap.
security_workflow="$(printf '%s\n' "$default_security_workflow" | grep -v 'build-tags')"
write_repo "$two_jobs_both_modules" nested
assert_rejected 'the gosec job without the tagged contour' "job gosec in .github/workflows/go-security.yml does not lint the root module with --build-tags=$tags"
security_workflow=$default_security_workflow

# A repository without go-security.yml has no code-scanning run to read.
write_repo "$two_jobs_both_modules" nested
rm "$work_dir/repo/.github/workflows/go-security.yml"
git -C "$work_dir/repo" add -A
assert_rejected 'no go-security.yml' '.github/workflows/go-security.yml not found'

printf 'go module lint coverage self-test: an unlinted module, one dropped from a single job, a workflow linting nothing, and a missing or incomplete tagged contour in either workflow are each reported\n'

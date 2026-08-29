#!/usr/bin/env bash
# Proves check-test-style.sh selects the right files, which is the half of this
# gate that is its own rather than the analyzer's.
#
# The failure it exists to stop was measured: `teststyle -root .` walks the
# filesystem and prunes by directory NAME only, and the root of a linked git
# worktree is an ordinary directory whose `.git` is a regular FILE holding a
# `gitdir:` pointer -- so nothing pruned it. The walk descended into every
# checkout parked under the repository, reported its tests as violations of this
# repository's baseline, and `--write-baseline` captured those foreign paths into
# the tracked baseline (stokaro/ptah#2509 moves the fixtures here).
#
# The analyzer's own rules are teststyle's to prove. What is asserted here is
# the selection, the refusal to scan nothing, and the announced skip.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-test-style.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-teststyle-selftest.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

write_repo() {
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/scripts"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-test-style.sh"
}

list_paths() {
	(cd "$work_dir/repo" && scripts/check-test-style.sh --list-scan-paths)
}

assert_lists() {
	local name=$1 path=$2
	if ! list_paths | grep -qFx "$path"; then
		printf 'test style self-test: %s -- %s was not selected:\n' "$name" "$path" >&2
		list_paths | sed 's/^/  /' >&2
		exit 1
	fi
}

assert_omits() {
	local name=$1 path=$2
	if list_paths | grep -qFx "$path"; then
		printf 'test style self-test: %s -- %s was selected and must not be\n' "$name" "$path" >&2
		exit 1
	fi
}

# A tracked test file, and an untracked one: the gate has to fire before
# `git add`, or it is green on the very file the author is about to commit.
write_repo
mkdir -p "$work_dir/repo/pkg"
printf 'package pkg\n' >"$work_dir/repo/pkg/tracked_test.go"
git -C "$work_dir/repo" add pkg/tracked_test.go
printf 'package pkg\n' >"$work_dir/repo/pkg/brand_new_test.go"
assert_lists 'a tracked test file' 'pkg/tracked_test.go'
assert_lists 'a test file not yet added' 'pkg/brand_new_test.go'

# A file that is not a test is not this gate's business.
printf 'package pkg\n' >"$work_dir/repo/pkg/production.go"
git -C "$work_dir/repo" add pkg/production.go
assert_omits 'a non-test file' 'pkg/production.go'

# The worktree, created with `git worktree add` rather than by writing a `.git`
# file by hand. That distinction IS the fixture: a hand-written marker pointing
# at a gitdir the parent does not know about is treated as an ordinary
# directory and git DOES list the files under it -- measured, while writing
# this. A registered worktree is excluded, which is the property the gate
# relies on; a fixture that faked the marker would have asserted the opposite
# and read as a defect in the gate.
git -C "$work_dir/repo" -c user.email=selftest@example.invalid -c user.name=selftest \
	commit -qm 'a parent needs a commit before it can carry a worktree'
git -C "$work_dir/repo" worktree add -q parked -b parked-selftest
mkdir -p "$work_dir/repo/parked/pkg"
printf 'package pkg\\n' >"$work_dir/repo/parked/pkg/foreign_test.go"
assert_omits 'a test file inside a parked checkout' 'parked/pkg/foreign_test.go'

# Selecting nothing is a broken gate, not a clean tree. This path returns before
# any Go tooling runs, so the fixture needs no module.
write_repo
if (cd "$work_dir/repo" && scripts/check-test-style.sh) >"$work_dir/out" 2>"$work_dir/err"; then
	printf 'test style self-test: a repository with no test files reported success\n' >&2
	exit 1
fi
if ! grep -qF 'refusing to report success' "$work_dir/err"; then
	printf 'test style self-test: an empty selection failed for the wrong reason:\n' >&2
	sed 's/^/  /' "$work_dir/err" >&2
	exit 1
fi

printf 'test style self-test: tracked and new files selected, a parked checkout and an ignored file are not, and an empty selection is refused\n'

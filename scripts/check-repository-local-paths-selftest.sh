#!/usr/bin/env bash
# Proves check-repository-local-paths.sh rejects a developer's own path, keeps
# the shapes it deliberately allows, and refuses a vacuous pass.
#
# The last row is the one worth having. `git grep` reports 1 both for "scanned
# everything and matched nothing" and for "scanned nothing", so without the
# file count this gate cannot tell a clean tree from a scan that stopped
# covering the repository (stokaro/ptah#2509).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-repository-local-paths.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-local-paths.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

# write_repo builds a throwaway repository holding one tracked file. The gate
# enumerates with `git ls-files`, so the fixture has to be committed.
write_repo() {
	local content=$1
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/scripts"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-repository-local-paths.sh"
	printf '%s\n' "$content" >"$work_dir/repo/note.md"
	git -C "$work_dir/repo" add -A
}

assert_rejected() {
	local name=$1
	if (cd "$work_dir/repo" && scripts/check-repository-local-paths.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'repository-local path self-test: %s unexpectedly passed\n' "$name" >&2
		exit 1
	fi
	if ! grep -qF 'repository-local paths found' "$work_dir/err"; then
		printf 'repository-local path self-test: %s failed for the wrong reason:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

assert_accepted() {
	local name=$1
	if ! (cd "$work_dir/repo" && scripts/check-repository-local-paths.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'repository-local path self-test: %s was rejected:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

# The fixtures are assembled from parts so this file does not itself contain a
# developer path. It is scanned by the gate it tests -- everything tracked is,
# apart from the gate's own source -- and a companion carrying the literal
# would be a leak nobody could tell from a real one.
mac_home="/Users"
linux_home="/home"
home_var='\$HOME'

write_repo "Run it from ${mac_home}/somebody/Work/thing."
assert_rejected 'a macOS home path'

write_repo "Run it from ${linux_home}/somebody/Work/thing."
assert_rejected 'a Linux home path'

write_repo "It lives in ${home_var}""/Work/ptah."
assert_rejected 'a \$HOME checkout convention'

# The shapes it must NOT reject. Each is a real spelling this repository
# contains, and a pattern that caught them would make the gate unusable.
write_repo "The Windows runner uses C:${mac_home}/runner/work, which is not a local path."
assert_accepted 'a synthetic Windows path in a cross-platform test'

write_repo "The container mounts ${linux_home}-grown/data and ${mac_home}-guide.md."
assert_accepted 'a path that merely starts with the same letters'

# The boundary, recorded rather than assumed: the pattern requires a trailing
# slash, so the directory itself is not matched. Whether it should be is a
# question about the pattern; this says what the pattern does, so the next
# reader does not have to derive it.
write_repo "The checkout usually sits at ${home_var}""/Work."
assert_accepted 'a \$HOME directory with no trailing slash'
# Vacuity. A repository with nothing tracked must fail rather than report a
# clean scan: the gate cannot otherwise tell an empty pathspec from a clean tree.
rm -rf "$work_dir/repo"
mkdir -p "$work_dir/repo/scripts"
git -C "$work_dir/repo" init --quiet
cp "$check" "$work_dir/repo/scripts/check-repository-local-paths.sh"
if (cd "$work_dir/repo" && scripts/check-repository-local-paths.sh) >"$work_dir/out" 2>"$work_dir/err"; then
	printf 'repository-local path self-test: an empty tree reported a clean scan\n' >&2
	exit 1
fi
grep -qF 'refusing to report a vacuous pass' "$work_dir/err"

printf 'repository-local path self-test: three local spellings rejected, three lookalikes kept, and an empty scan refused\n'

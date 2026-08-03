#!/usr/bin/env sh
set -eu

usage() {
	echo "usage: $0 [--list-scan-paths|--write-baseline]" >&2
}

mode=check
case "${1-}" in
"") ;;
--list-scan-paths) mode=list ;;
--write-baseline) mode=write ;;
*)
	usage
	exit 2
	;;
esac

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# select_test_files prints every test file this gate is allowed to judge, one per
# line, relative to the repository root.
#
# The scanner is never handed a directory to walk. `teststyle -root .` walks the
# filesystem and prunes by directory *name* only (.git, vendor, node_modules,
# testdata), and the root of a linked git worktree is an ordinary directory whose
# `.git` is a regular *file* holding a `gitdir:` pointer -- nothing prunes it. The
# walk therefore descended into every checkout parked under the repo and reported
# its tests as violations of this repo's baseline, so the gate was red for code
# that is not in the working tree at all, and `-write-baseline` captured those
# foreign paths into the tracked baseline.
#
# git is the authority on what belongs to this checkout: it refuses to descend
# past a nested `.git` marker, so no worktree path can appear here regardless of
# ignore rules. This mirrors the testify check below, which searches tracked
# files only for the same reason.
#
#   --cached                    tracked files
#   --others --exclude-standard brand-new local test files, so the gate still
#                               fires before `git add`. Dropping these would make
#                               the gate green on the very file the author is
#                               about to commit.
#   core.quotePath=false        emit non-ASCII paths raw instead of C-quoted, so
#                               they survive the line-based read below
#
# A future submodule would appear here as a gitlink rather than as its files; it
# would need a scan of its own.
select_test_files() {
	git -c core.quotePath=false ls-files --cached --others --exclude-standard -- '*_test.go'
}

if [ "$mode" = list ]; then
	select_test_files
	exit 0
fi

# git grep rather than rg: when ripgrep is absent the command exits 127, the
# `if` reads that as "no matches", and `set -e` does not fire inside a condition
# -- so this gate reported success without ever running. It did exactly that on
# CI, which has no ripgrep. git grep is always present here and searches tracked
# files only, which also keeps stray worktrees out of the result.
if git grep -nE 'github\.com/stretchr/testify|\b(assert|require)\.' -- '*.go'; then
	echo "teststyle: testify/assert/require usage is prohibited; use quicktest as qt instead" >&2
	exit 1
fi

go_cache="${GOCACHE:-$(go env GOCACHE)}"
if ! mkdir -p "$go_cache" 2>/dev/null || [ ! -w "$go_cache" ]; then
	GOCACHE="${TMPDIR:-/tmp}/ptah-go-cache"
	export GOCACHE
	mkdir -p "$GOCACHE"
fi

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-teststyle.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

scan_root="$work_dir/root"
path_list="$work_dir/paths"
mkdir -p "$scan_root"

# Materialize the list first so a failure of git itself stops the gate. Piping
# straight into the loop would hide it: a pipeline's status is its last command's,
# so a broken `git ls-files` would scan nothing and still report success.
select_test_files >"$path_list"

if [ ! -s "$path_list" ]; then
	echo "teststyle: no test files selected from git; refusing to report success" >&2
	exit 1
fi

while IFS= read -r path; do
	# Deliberate skip, not an oversight: a staged-but-deleted file, or a path whose
	# name defeats line-based reading, must not abort the whole gate. It is
	# announced rather than swallowed.
	if [ ! -f "$path" ]; then
		echo "teststyle: skipping $path (not a regular file in the working tree)" >&2
		continue
	fi
	mkdir -p "$scan_root/$(dirname "$path")"
	cp "$path" "$scan_root/$path"
done <"$path_list"

# teststyle reports paths relative to -root, so the temporary root leaves baseline
# keys unchanged. -baseline must be absolute: the CLI resolves it against the
# working directory, not against -root.
if [ "$mode" = write ]; then
	GOWORK=off go tool teststyle -baseline "$repo_root/.teststyle-baseline.json" -write-baseline -root "$scan_root"
	exit 0
fi

GOWORK=off go tool teststyle -baseline "$repo_root/.teststyle-baseline.json" -root "$scan_root"

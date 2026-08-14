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

# The library is resolved from this script's own location, never from the
# repository the script is pointed at. internal/teststyleguard runs this script
# with its working directory inside a throwaway fixture repository, so
# `git rev-parse --show-toplevel` answers with that fixture -- which has no
# scripts/ directory at all, and sourcing through it made the gate fail with
# "No such file or directory" for every fixture row.
script_dir="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"

# select_test_files is shared with scripts/check-quicktest-shape.sh, which needs
# exactly the same answer to exactly the same question and must not grow a second,
# divergent idea of which files belong to this checkout. The reasoning behind the
# git-based selection -- and the linked-worktree failure it exists to prevent --
# is recorded there.
#
# shellcheck source=scripts/lib/select-test-files.sh
. "$script_dir/lib/select-test-files.sh"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [ "$mode" = list ]; then
	select_test_files
	exit 0
fi

# The testify prohibition used to live here as
#
#   git grep -nE 'github\.com/stretchr/testify|\b(assert|require)\.' -- '*.go'
#
# and it is now a depguard deny entry in .golangci.yml. The text scan could not
# tell a call from a sentence -- `\b(assert|require)\.` is "a word, then a full
# stop" -- and `\b`
# meant "word boundary" to the regex engine git grep compiles with on Linux and
# the literal letter `b` to the one it compiles with on macOS, so the same tree
# was refused on CI for a comment and accepted locally for a real call. See
# stokaro/ptah#1139. depguard matches the import declaration, which is a call
# site by construction and has no regex flavor to vary.

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

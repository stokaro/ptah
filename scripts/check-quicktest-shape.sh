#!/usr/bin/env sh
# Enforces the two quicktest shape rules from AGENTS.md:
#
#   R1  the package-level qt.Assert / qt.Check are forbidden; assert through a
#       *qt.C created from the testing.TB that owns the scope
#   R2  c.Run is forbidden; subtests are t.Run(name, func(t *testing.T)) with a
#       fresh qt.New inside. The callback may be named elsewhere, the receiver
#       may be any expression, and the method may be referenced without being
#       called there and then; all of those are the same subtest
#   R3  a t.Run, b.Run or f.Fuzz closure may not assert through a checker, or
#       build one from a testing.TB, that belongs to the scope outside it, and
#       the qt.New it does build must take the testing.TB it was handed
#
# There is no baseline and no exemption mechanism, on purpose. scripts/check-test-style.sh
# carries one because it is cleaning up a pre-existing backlog under issue #541;
# these two rules were applied to the whole tree in the same change that added
# this gate, so a baseline here would have nothing legitimate to record and would
# only grandfather the next violation.
set -eu

usage() {
	echo "usage: $0 [--list-scan-paths]" >&2
}

mode=check
case "${1-}" in
"") ;;
--list-scan-paths) mode=list ;;
*)
	usage
	exit 2
	;;
esac

# Resolved from this script's own location, never from the repository the script
# is pointed at: a gate script may be run with its working directory inside a
# throwaway fixture repository that has no scripts/ directory.
script_dir="$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)"

# shellcheck source=scripts/lib/select-test-files.sh
. "$script_dir/lib/select-test-files.sh"

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [ "$mode" = list ]; then
	select_test_files
	exit 0
fi

go_cache="${GOCACHE:-$(go env GOCACHE)}"
if ! mkdir -p "$go_cache" 2>/dev/null || [ ! -w "$go_cache" ]; then
	GOCACHE="${TMPDIR:-/tmp}/ptah-go-cache"
	export GOCACHE
	mkdir -p "$GOCACHE"
fi

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-qtshape.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM
path_list="$work_dir/paths"

# Materialize the list first so a failure of git itself stops the gate. Piping
# straight into the scanner would hide it: a pipeline's status is its last
# command's, so a broken `git ls-files` would scan nothing and the scanner's exit
# code would be the only thing read.
select_test_files >"$path_list"

if [ ! -s "$path_list" ]; then
	echo "qtshape: no test files selected from git; refusing to report success" >&2
	exit 2
fi

GOWORK=off go run ./internal/cmd/qtshape <"$path_list"

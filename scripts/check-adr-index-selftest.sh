#!/usr/bin/env bash
# Proves check-adr-index.sh rejects each shape it exists to reject.
#
# Five detectors, five known-bad fixtures and a control. The gate's own failure
# was two records shipping as ADR 0006, each pull request green because nothing
# compared the two -- so the duplicate-number row below is the one that matters
# most, and the control is what keeps a gate that refused everything from
# satisfying it (stokaro/ptah#2509).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
check="$repo_root/scripts/check-adr-index.sh"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-adr-index.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

# write_repo builds a throwaway repository holding one ADR directory. The gate
# asks git for the root, so the fixture has to be a repository rather than a
# directory.
write_repo() {
	rm -rf "$work_dir/repo"
	mkdir -p "$work_dir/repo/docs/adr" "$work_dir/repo/scripts"
	git -C "$work_dir/repo" init --quiet
	cp "$check" "$work_dir/repo/scripts/check-adr-index.sh"
}

record() {
	local name=$1 heading=$2
	printf '%s\n\nA record.\n' "$heading" >"$work_dir/repo/docs/adr/$name"
}

index() {
	{
		printf '# Architecture decision records\n\n'
		for row in "$@"; do
			printf -- '- [%s](%s)\n' "$row" "$row"
		done
	} >"$work_dir/repo/docs/adr/README.md"
}

assert_rejected() {
	local name=$1 expected=$2
	if (cd "$work_dir/repo" && scripts/check-adr-index.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'ADR index self-test: %s unexpectedly passed\n' "$name" >&2
		exit 1
	fi
	if ! grep -qF "$expected" "$work_dir/err"; then
		printf 'ADR index self-test: %s failed for the wrong reason:\n' "$name" >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

assert_accepted() {
	if ! (cd "$work_dir/repo" && scripts/check-adr-index.sh) >"$work_dir/out" 2>"$work_dir/err"; then
		printf 'ADR index self-test: the control was rejected:\n' >&2
		sed 's/^/  /' "$work_dir/err" >&2
		exit 1
	fi
}

# D1: a number that is not four digits.
write_repo
record '007-short.md' '# ADR 007: Short'
index '007-short.md'
assert_rejected 'a three-digit number' 'must begin with a four-digit number'

# D2: two records sharing a number. This is the failure the gate was written
# for, and the one that shipped.
write_repo
record '0006-first.md' '# ADR 0006: First'
record '0006-second.md' '# ADR 0006: Second'
index '0006-first.md' '0006-second.md'
assert_rejected 'two records with one number' 'ADR 0006 is used by more than one record'

# D3: a renumbering that renamed the file and left the heading behind.
write_repo
record '0007-renumbered.md' '# ADR 0006: Renumbered'
index '0007-renumbered.md'
assert_rejected 'a heading naming another number' "must be '# ADR 0007: ...'"

# D4: a record the index does not list. This is the half of the #2064/#2070
# collision that made one record unreachable.
write_repo
record '0006-listed.md' '# ADR 0006: Listed'
record '0007-unlisted.md' '# ADR 0007: Unlisted'
index '0006-listed.md'
assert_rejected 'a record with no index row' 'no index row links to 0007-unlisted.md'

# D5: an index row pointing at a record that is not there.
write_repo
record '0006-present.md' '# ADR 0006: Present'
index '0006-present.md' '0007-absent.md'
assert_rejected 'an index row with no record' 'index row links to 0007-absent.md'

# The control. Without it every row above is satisfied by a gate that refuses
# everything, which is the failure mode a self-test exists to rule out.
write_repo
record '0001-first.md' '# ADR 0001: First'
record '0002-second.md' '# ADR 0002: Second'
index '0001-first.md' '0002-second.md'
assert_accepted

printf 'ADR index self-test: five detectors each reject their own shape, and a well-formed index passes\n'

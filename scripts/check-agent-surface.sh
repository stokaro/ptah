#!/usr/bin/env bash
# Checks the generated agent-surface tables against the command tree they describe.
#
# docs/agent-surface.md restates two things: every verb with what it does to a
# database, and the shortlist an agent-exposure decision starts from. Both are
# derived — from the cobra tree the binary is built out of, joined with the
# classification in internal/agentsurface — and the Go guards there already
# refuse a classification that drifts from the tree. What they cannot see is the
# document, which is the copy people read (stokaro/ptah#1484).
#
# `--write` regenerates the blocks instead of checking them.
#
# The marker and emptiness checks are not defensive padding. A file whose
# markers were renamed or lost in a merge would yield an empty block on both
# sides, the comparison would find them identical, and a gate that compares
# nothing to nothing reports success at exactly the moment it stopped working.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

mode="check"
case "${1-}" in
"") ;;
--write) mode="write" ;;
*)
	echo "usage: $0 [--write]" >&2
	exit 2
	;;
esac

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-agent-surface.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

go run ./internal/cmd/agentsurface >"$work_dir/all.md"
go run ./internal/cmd/agentsurface --database-safe >"$work_dir/safe.md"

status=0

check_block() {
	local target="$1" begin="$2" end="$3" generated="$4" source="$5"

	if [ ! -s "$generated" ]; then
		echo "check-agent-surface: the generator produced nothing for $begin; refusing to compare a document against an empty table" >&2
		return 1
	fi
	if ! grep -qF "$begin" "$target" || ! grep -qF "$end" "$target"; then
		echo "check-agent-surface: $target carries no $begin markers" >&2
		return 1
	fi

	local extracted="$work_dir/extracted"
	awk -v begin="$begin" -v end="$end" '
		$0 == end { inside = 0 }
		inside { print }
		$0 == begin { inside = 1 }
	' "$target" >"$extracted"

	if [ "$mode" = write ]; then
		python3 - "$target" "$generated" "$begin" "$end" <<-'PY'
			import sys

			target, generated, begin, end = sys.argv[1:5]
			body = open(target).read()
			table = open(generated).read()
			head, _, rest = body.partition(begin + "\n")
			_, _, tail = rest.partition(end)
			open(target, "w").write(head + begin + "\n" + table + end + tail)
		PY
		echo "check-agent-surface: rewrote $begin in $target"
		return 0
	fi

	if ! diff -u "$generated" "$extracted" >"$work_dir/diff"; then
		echo "check-agent-surface: $target is out of date with $source" >&2
		echo "check-agent-surface: run scripts/check-agent-surface.sh --write" >&2
		sed 's/^/  /' "$work_dir/diff" >&2
		return 1
	fi
	return 0
}

check_block docs/agent-surface.md \
	'<!-- BEGIN GENERATED AGENT SURFACE -->' \
	'<!-- END GENERATED AGENT SURFACE -->' \
	"$work_dir/all.md" \
	"the command tree and internal/agentsurface" || status=1

check_block docs/agent-surface.md \
	'<!-- BEGIN GENERATED DATABASE-SAFE VERBS -->' \
	'<!-- END GENERATED DATABASE-SAFE VERBS -->' \
	"$work_dir/safe.md" \
	"the command tree and internal/agentsurface" || status=1

# A second copy of either table elsewhere would go stale unchecked, so the
# markers are required to be unique to the file listed above.
for begin in '<!-- BEGIN GENERATED AGENT SURFACE -->' '<!-- BEGIN GENERATED DATABASE-SAFE VERBS -->'; do
	carriers="$(git grep -l -F "$begin" -- '*.md' '*.mdx' || true)"
	if [ "$carriers" != "docs/agent-surface.md" ]; then
		echo "check-agent-surface: $begin appears in files this script does not check:" >&2
		echo "$carriers" | sed 's/^/  /' >&2
		status=1
	fi
done

if [ "$status" -eq 0 ] && [ "$mode" = check ]; then
	echo "check-agent-surface: both generated tables match the command tree"
fi
exit "$status"

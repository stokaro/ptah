#!/usr/bin/env bash
# Checks the documented version matrix against the declaration it is generated
# from.
#
# The supported set is written down in three places: the release lines declared
# in internal/capabilityprobe/cells.go, the CI fan-out both tiers of
# stokaro/ptah#1341 compute from them, and the documentation matrix a reader
# consults. The first two share a source by construction -- the workflows carry
# no list of versions and ask `capmatrix matrix` instead. This script is what
# ties the third to the same source, so a line added to the declaration cannot
# stay undocumented and a line deleted from the declaration cannot stay
# documented.
#
# `--write` regenerates the blocks instead of checking them.
#
# The marker check is not defensive padding. A file whose markers were renamed
# or lost in a merge would yield an empty block on both sides, the comparison
# would find them identical, and a gate that compares nothing to nothing
# reports success at exactly the moment it stopped working.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

mode=check
case "${1-}" in
"") ;;
--write) mode=write ;;
*)
	echo "usage: $0 [--write]" >&2
	exit 2
	;;
esac

readonly BEGIN='<!-- BEGIN GENERATED VERSION MATRIX -->'
readonly END='<!-- END GENERATED VERSION MATRIX -->'

# Every file that carries the generated block, and which rendering it carries.
# A file added here is checked the moment it is listed; a file that carries the
# markers and is NOT listed is caught below, so the list cannot silently fall
# behind the documentation.
#
# Two renderings, one declaration. The site's responsive check refuses a table
# wider than its reading column, so the site carries a narrow view of the same
# cells with the rest written out underneath. Both come from the same command.
targets=(
	"docs/capabilities.md:wide"
	"docs/site/src/content/docs/databases/support-matrix.md:compact"
)

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-version-matrix.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

go run ./internal/cmd/capmatrix markdown >"$work_dir/wide.md"
go run ./internal/cmd/capmatrix markdown --compact >"$work_dir/compact.md"

for rendering in wide compact; do
	if [ ! -s "$work_dir/$rendering.md" ]; then
		echo "check-version-matrix: the $rendering rendering produced nothing; refusing to compare a document against an empty table" >&2
		exit 1
	fi
done

# carriers lists every tracked Markdown file holding the begin marker. It is
# derived rather than assumed so that a third copy of the table cannot appear
# unchecked.
carriers="$work_dir/carriers"
git grep -l -F "$BEGIN" -- '*.md' '*.mdx' >"$carriers" || true

status=0

for entry in "${targets[@]}"; do
	target="${entry%:*}"
	generated="$work_dir/${entry##*:}.md"
	if ! grep -qF "$BEGIN" "$target" || ! grep -qF "$END" "$target"; then
		echo "check-version-matrix: $target carries no generated-matrix markers" >&2
		status=1
		continue
	fi
	extracted="$work_dir/$(echo "$target" | tr '/' '_')"
	awk -v begin="$BEGIN" -v end="$END" '
		$0 == end { inside = 0 }
		inside { print }
		$0 == begin { inside = 1 }
	' "$target" >"$extracted"

	if [ "$mode" = write ]; then
		python3 - "$target" "$generated" "$BEGIN" "$END" <<-'PY'
			import sys

			target, generated, begin, end = sys.argv[1:5]
			body = open(target).read()
			table = open(generated).read()
			head, _, rest = body.partition(begin + "\n")
			_, _, tail = rest.partition(end)
			open(target, "w").write(head + begin + "\n" + table + end + tail)
		PY
		echo "check-version-matrix: rewrote the generated block in $target"
		continue
	fi

	if ! diff -u "$generated" "$extracted" >"$work_dir/diff"; then
		echo "check-version-matrix: $target is out of date with internal/capabilityprobe/cells.go" >&2
		echo "check-version-matrix: run scripts/check-version-matrix.sh --write" >&2
		sed 's/^/  /' "$work_dir/diff" >&2
		status=1
	fi
done

while IFS= read -r carrier; do
	[ -n "$carrier" ] || continue
	found=0
	for entry in "${targets[@]}"; do
		[ "$carrier" = "${entry%:*}" ] && found=1
	done
	if [ "$found" -eq 0 ]; then
		echo "check-version-matrix: $carrier carries the generated matrix and is not in this script's list" >&2
		status=1
	fi
done <"$carriers"

if [ "$status" -eq 0 ] && [ "$mode" = check ]; then
	echo "check-version-matrix: OK (${#targets[@]} documents match the declared release lines)"
fi

exit "$status"

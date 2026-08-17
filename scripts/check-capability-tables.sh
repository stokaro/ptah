#!/usr/bin/env bash
# Checks the documented capability tables against the registry they describe.
#
# Two tables in docs/capabilities.md restate what core/platform/capability
# declares: the key-to-meaning registry, and the capability-by-preset matrix.
# Both were hand-maintained, and both had fallen behind in the direction nobody
# notices -- five keys missing from each, four presets missing from the matrix,
# and a SQL Server column declaring two IF EXISTS guards absent that every
# supported line accepts. A table with one cell per (key, preset) has a few
# hundred cells, which is more than a reviewer can check by reading, so the
# check is mechanical.
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

mode="check"
case "${1-}" in
"") ;;
--write) mode="write" ;;
*)
	echo "usage: $0 [--write]" >&2
	exit 2
	;;
esac

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-capability-tables.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

go run ./internal/cmd/capmatrix markdown --keys >"$work_dir/keys.md"
go run ./internal/cmd/capmatrix markdown --presets >"$work_dir/presets.md"

status=0

check_block() {
	local target="$1" begin="$2" end="$3" generated="$4" source="$5"

	if [ ! -s "$generated" ]; then
		echo "check-capability-tables: the generator produced nothing for $begin; refusing to compare a document against an empty table" >&2
		return 1
	fi
	if ! grep -qF "$begin" "$target" || ! grep -qF "$end" "$target"; then
		echo "check-capability-tables: $target carries no $begin markers" >&2
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
		echo "check-capability-tables: rewrote $begin in $target"
		return 0
	fi

	if ! diff -u "$generated" "$extracted" >"$work_dir/diff"; then
		echo "check-capability-tables: $target is out of date with $source" >&2
		echo "check-capability-tables: run scripts/check-capability-tables.sh --write" >&2
		sed 's/^/  /' "$work_dir/diff" >&2
		return 1
	fi
	return 0
}

check_block docs/capabilities.md \
	'<!-- BEGIN GENERATED CAPABILITY KEYS -->' \
	'<!-- END GENERATED CAPABILITY KEYS -->' \
	"$work_dir/keys.md" \
	"the capability registry" || status=1

check_block docs/capabilities.md \
	'<!-- BEGIN GENERATED PRESET MATRIX -->' \
	'<!-- END GENERATED PRESET MATRIX -->' \
	"$work_dir/presets.md" \
	"capability.NamedPresets" || status=1

# A second copy of either table elsewhere would go stale unchecked, so the
# markers are required to be unique to the file listed above.
for begin in '<!-- BEGIN GENERATED CAPABILITY KEYS -->' '<!-- BEGIN GENERATED PRESET MATRIX -->'; do
	carriers="$(git grep -l -F "$begin" -- '*.md' '*.mdx' || true)"
	if [ "$carriers" != "docs/capabilities.md" ]; then
		echo "check-capability-tables: $begin appears in files this script does not check:" >&2
		echo "$carriers" | sed 's/^/  /' >&2
		status=1
	fi
done

if [ "$status" -eq 0 ] && [ "$mode" = check ]; then
	echo "check-capability-tables: both generated tables match the registry"
fi
exit "$status"

#!/usr/bin/env bash
# Checks the documented lint-rule enumeration against the registries it is
# generated from.
#
# The rules are declared in two places the linters actually read:
# migration/lint's rule registry and internal/sqllint's catalog. The reference
# page a user consults is a third list, and a third list drifts. This script is
# what ties it to the other two, so a rule added to the code cannot stay
# undocumented and a row for a rule that no longer exists cannot stay on the
# page.
#
# `--write` regenerates the block instead of checking it.
#
# The marker check is not defensive padding. A file whose markers were renamed
# or lost in a merge would yield an empty block on both sides, the comparison
# would find them identical, and a gate that compares nothing to nothing reports
# success at exactly the moment it stopped working.
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

readonly BEGIN='<!-- BEGIN GENERATED LINT RULES -->'
readonly END='<!-- END GENERATED LINT RULES -->'

# The one page that carries the enumeration. A file that carries the markers and
# is not listed here is caught below, so the list cannot silently fall behind the
# documentation.
targets=(
	"docs/site/src/content/docs/reference/lint-rules.md"
)

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-lint-rules.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

# The generator validates before it prints, so a catalog that disagrees with the
# registries fails here rather than being rendered into the page.
go run ./internal/cmd/lintrules markdown >"$work_dir/generated.md"

if [ ! -s "$work_dir/generated.md" ]; then
	echo "check-lint-rules: the generator produced nothing; refusing to compare a document against an empty enumeration" >&2
	exit 1
fi

# carriers lists every tracked Markdown file holding the begin marker. It is
# derived rather than assumed so that a second copy of the enumeration cannot
# appear unchecked.
carriers="$work_dir/carriers"
git grep -l -F "$BEGIN" -- '*.md' '*.mdx' >"$carriers" || true

status=0

for target in "${targets[@]}"; do
	if ! grep -qF "$BEGIN" "$target" || ! grep -qF "$END" "$target"; then
		echo "check-lint-rules: $target carries no generated-rules markers" >&2
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
		python3 - "$target" "$work_dir/generated.md" "$BEGIN" "$END" <<-'PY'
			import sys

			target, generated, begin, end = sys.argv[1:5]
			body = open(target).read()
			table = open(generated).read()
			head, _, rest = body.partition(begin + "\n")
			_, _, tail = rest.partition(end)
			open(target, "w").write(head + begin + "\n" + table + end + tail)
		PY
		echo "check-lint-rules: rewrote the generated block in $target"
		continue
	fi

	if ! diff -u "$work_dir/generated.md" "$extracted" >"$work_dir/diff"; then
		echo "check-lint-rules: $target is out of date with the lint rule registries" >&2
		echo "check-lint-rules: run scripts/check-lint-rules.sh --write" >&2
		sed 's/^/  /' "$work_dir/diff" >&2
		status=1
	fi
done

while IFS= read -r carrier; do
	[ -n "$carrier" ] || continue
	found=0
	for target in "${targets[@]}"; do
		[ "$carrier" = "$target" ] && found=1
	done
	if [ "$found" -eq 0 ]; then
		echo "check-lint-rules: $carrier carries the generated enumeration and is not in this script's list" >&2
		status=1
	fi
done <"$carriers"

if [ "$status" -eq 0 ] && [ "$mode" = check ]; then
	rules="$(go run ./internal/cmd/lintrules check)"
	echo "check-lint-rules: OK (${#targets[@]} document matches the rule registries; ${rules#lintrules: OK })"
fi

exit "$status"

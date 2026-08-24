#!/usr/bin/env bash

set -euo pipefail

# Every architecture decision record has a number of its own, and the index
# lists every one of them.
#
# The failure this exists to stop happened: two records shipped as ADR 0006.
# #2064 added 0006-one-authorized-agent-runtime.md and #2070, branched before it
# and rebased after, added 0006-agent-error-taxonomy.md. Each pull request was
# green, because nothing compared the two, and the index row for the first was
# lost in the rebase that carried the second -- so master held two records with
# one number and one of them was unreachable from the index that is supposed to
# list them all. Three other records cite "ADR 0006" by link, and which record
# they meant became a question the tree could not answer.
#
# The detectors enumerate the FILES and require the index to match, rather than
# reading the index and hoping the files agree. A record added without a row
# therefore fails on arrival.

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

adr_dir="docs/adr"
index="$adr_dir/README.md"
status=0

fail() {
	printf '::error file=%s::%s\n' "$1" "$2" >&2
	status=1
}

if [ ! -f "$index" ]; then
	fail "$adr_dir" "the ADR index $index does not exist"
	exit 1
fi

seen_numbers=""

for record in "$adr_dir"/[0-9]*.md; do
	base="$(basename "$record")"
	number="${base%%-*}"

	# D1: the number is four digits, so the sort order and the links agree.
	case "$number" in
	[0-9][0-9][0-9][0-9]) ;;
	*)
		fail "$record" "ADR file name must begin with a four-digit number: $base"
		continue
		;;
	esac

	# D2: no two records share a number.
	case " $seen_numbers " in
	*" $number "*) fail "$record" "ADR $number is used by more than one record" ;;
	*) seen_numbers="$seen_numbers $number" ;;
	esac

	# D3: the title inside the record names the same number as the file, so a
	# renumbering that renames the file and forgets the heading is caught.
	if ! head -1 "$record" | grep -qE "^# ADR $number:"; then
		fail "$record" "first line must be '# ADR $number: ...', found: $(head -1 "$record")"
	fi

	# D4: the index has a row linking to this record.
	if ! grep -qF "($base)" "$index"; then
		fail "$index" "no index row links to $base"
	fi
done

# D5: every row in the index points at a record that exists.
while IFS= read -r linked; do
	if [ ! -f "$adr_dir/$linked" ]; then
		fail "$index" "index row links to $linked, which does not exist"
	fi
done < <(grep -oE '\(([0-9]{4}-[^)]+\.md)\)' "$index" | tr -d '()')

if [ "$status" -eq 0 ]; then
	echo "ADR index: OK ($(echo "$seen_numbers" | wc -w | tr -d ' ') records)"
fi

exit "$status"

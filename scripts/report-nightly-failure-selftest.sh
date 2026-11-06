#!/usr/bin/env bash
# Proves report-nightly-failure.sh keeps one open issue for failed nightly runs.
#
# The script runs only on a night something failed, and a broken report looks
# the same as a quiet night. So each case puts a stub gh first on PATH, answers
# the listing from the case's row, and requires exactly the write the row names.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
title='Nightly run of "Atlas CE reference" failed'
create="issue create --title $title --label bug --body-file -"

dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
mkdir "$dir/bin"
cat >"$dir/bin/gh" <<'STUB'
#!/usr/bin/env bash
if [ "$1 $2" = "issue list" ]; then
	[ -z "${STUB_LIST_FAILS:-}" ] || exit 1
	printf '%s\n' "$STUB_ISSUES"
	exit 0
fi
printf 'WRITE %s\n' "$*" >>"$STUB_LOG"
cat >>"$STUB_LOG"
STUB
chmod +x "$dir/bin/gh"

readonly needs='{"scope":{"result":"success"},"reference-from-source":{"result":"cancelled"},"differential-and-corpus":{"result":"skipped"},"complete-integration":{"result":"failure"}}'
readonly listed='Failed jobs: reference-from-source, complete-integration'

failures=0
cases=0

fail() {
	printf 'report-nightly-failure-selftest: %s: %s\n' "$1" "$2" >&2
	failures=$((failures + 1))
}

run_report() {
	: >"$dir/log"
	PATH="$dir/bin:$PATH" STUB_LOG="$dir/log" STUB_ISSUES="$1" \
		RUN_URL=https://example.test/run/1 COMMIT=abc123 NEEDS="$needs" \
		"$script_dir/report-nightly-failure.sh" >/dev/null 2>&1
}

# expect_write requires one write, the one named, carrying the run's details.
expect_write() {
	local name="$1" issues="$2" want="$3"
	cases=$((cases + 1))
	run_report "$issues" || fail "$name" "the script exited non-zero"
	local writes
	writes="$(grep '^WRITE ' "$dir/log" || true)"
	[ "$writes" = "WRITE $want" ] || fail "$name" "wrote [$writes], want [WRITE $want]"
	local detail
	for detail in https://example.test/run/1 abc123 "$listed"; do
		grep -qF "$detail" "$dir/log" || fail "$name" "the text leaves out $detail"
	done
	grep -qx "$listed" "$dir/log" || fail "$name" "the failed jobs read otherwise than [$listed]"
}

other="$(jq -cn '[{number: 5, title: "Another issue"}]')"
open="$(jq -cn --arg title "$title" '[{number: 5, title: "Another issue"}, {number: 7, title: $title}]')"
two="$(jq -cn --arg title "$title" '[{number: 9, title: $title}, {number: 7, title: $title}]')"
similar="$(jq -cn --arg title "$title again" '[{number: 7, title: $title}]')"

expect_write 'no issue is open' '[]' "$create"
expect_write 'only other issues are open' "$other" "$create"
expect_write 'the issue is open' "$open" 'issue comment 7 --body-file -'
expect_write 'a similar title is another issue' "$similar" "$create"
expect_write 'two are open, the older takes the comment' "$two" 'issue comment 7 --body-file -'

cases=$((cases + 1))
: >"$dir/log"
if PATH="$dir/bin:$PATH" STUB_LOG="$dir/log" STUB_ISSUES='[]' STUB_LIST_FAILS=1 \
	RUN_URL=https://example.test/run/1 COMMIT=abc123 NEEDS="$needs" \
	"$script_dir/report-nightly-failure.sh" >/dev/null 2>&1; then
	fail 'the listing fails' 'the script exited zero'
fi
[ ! -s "$dir/log" ] || fail 'the listing fails' 'the script wrote something'

if [ "$failures" -ne 0 ]; then
	printf 'report-nightly-failure-selftest: %d failures in %d cases\n' "$failures" "$cases" >&2
	exit 1
fi
printf 'report-nightly-failure-selftest: OK (%d cases)\n' "$cases"

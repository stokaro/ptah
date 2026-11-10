#!/usr/bin/env bash
# Proves watch.sh opens the issue each upstream event calls for, opens it once,
# and writes nothing on a quiet day or on an input it could not read.
#
# The watch runs nightly against live services, and each event it exists for
# happens once. A watch that stopped seeing them looks exactly like a quiet
# night. So each case feeds watch.sh fixtures in place of Gerrit, GitHub and the
# toolchain source, puts a stub gh first on PATH, and requires exactly the
# write the case names.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/go-runtime-patch/upstream.sh
. "$here/upstream.sh"
toolchain="$(sed -n 's/^toolchain //p' "$here/../../go.mod")"

dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT
mkdir "$dir/bin"
cat >"$dir/bin/gh" <<'STUB'
#!/usr/bin/env bash
printf 'CALL %s\n' "$*" >>"$STUB_LOG"
if [ "$1 $2" = "issue list" ]; then
	[ -z "${STUB_LIST_FAILS:-}" ] || exit 1
	printf '%s\n' "$STUB_ISSUES"
	exit 0
fi
cat >>"$STUB_LOG"
STUB
chmod +x "$dir/bin/gh"

readonly other_revision=0123456789abcdef0123456789abcdef01234567
readonly subject="the Go runtime patch for golang/go#$GO_RUNTIME_PATCH_ISSUE"
readonly cleanup="Remove $subject: $toolchain carries the fix"
readonly refresh="Refresh $subject: CL $GO_RUNTIME_PATCH_CHANGE is at patch set 4"
readonly abandoned="Reassess $subject: CL $GO_RUNTIME_PATCH_CHANGE was abandoned"
readonly closed="Reassess $subject: golang/go#$GO_RUNTIME_PATCH_ISSUE closed without CL $GO_RUNTIME_PATCH_CHANGE"

change_json() {
	local file="$dir/change-$1-$2.json"
	printf ")]}'\n" >"$file"
	jq -cn --arg status "$1" --arg rev "$2" --argjson ps "$3" \
		'{status: $status, current_revision: $rev, revisions: {($rev): {_number: $ps}}}' >>"$file"
	printf '%s\n' "$file"
}
printf 'package runtime\n\n%s n uintptr) {}\n' "$GO_RUNTIME_PATCH_MARKER" >"$dir/stack-fixed.go"
printf 'package runtime\n' >"$dir/stack-stock.go"
: >"$dir/stack-empty.go"
printf 'not the vendored patch\n' >"$dir/other.patch"
printf ")]}'\n{\"current_revision\": \"%s\"}\n" "$GO_RUNTIME_PATCH_REVISION" >"$dir/no-status.json"

pending="$(change_json NEW "$GO_RUNTIME_PATCH_REVISION" "$GO_RUNTIME_PATCH_PATCHSET")"
merged="$(change_json MERGED "$GO_RUNTIME_PATCH_REVISION" "$GO_RUNTIME_PATCH_PATCHSET")"
gone="$(change_json ABANDONED "$GO_RUNTIME_PATCH_REVISION" "$GO_RUNTIME_PATCH_PATCHSET")"
moved="$(change_json NEW "$other_revision" 4)"
merged_moved="$(change_json MERGED "$other_revision" 4)"
unknown="$(change_json DRAFT "$GO_RUNTIME_PATCH_REVISION" "$GO_RUNTIME_PATCH_PATCHSET")"
vendored="$here/$GO_RUNTIME_PATCH_FILE"
readonly none='[]'
listed="$(jq -cn --arg title "$cleanup" '[{number: 5, title: "Another issue"}, {number: 7, title: $title}]')"
similar="$(jq -cn --arg title "$cleanup again" '[{number: 7, title: $title}]')"

failures=0
cases=0

fail() {
	printf 'watch-selftest: %s: %s\n' "$1" "$2" >&2
	failures=$((failures + 1))
}

# run_watch CHANGE PATCH ISSUE STACK ISSUES [ARG]: sets status, fills out and log.
run_watch() {
	: >"$dir/log"
	status=0
	PATH="$dir/bin:$PATH" STUB_LOG="$dir/log" STUB_ISSUES="$5" \
		GO_RUNTIME_PATCH_CHANGE_JSON="$1" GO_RUNTIME_PATCH_UPSTREAM_PATCH="$2" \
		GO_RUNTIME_PATCH_ISSUE_STATE="$3" GO_RUNTIME_PATCH_TOOLCHAIN_STACK="$dir/stack-$4.go" \
		RUN_URL=https://example.test/run/1 GITHUB_STEP_SUMMARY='' \
		"$here/watch.sh" ${6:+"$6"} >"$dir/out" 2>&1 || status=$?
}

# expect_create requires a listing, then the one create the case names, with
# its label and the run that reported it.
expect_create() {
	local name="$1" title="$2" label="$3" body_text="$4"
	shift 4
	cases=$((cases + 1))
	run_watch "$@" --open-issue
	[ "$status" -eq 0 ] || fail "$name" "exited $status: $(cat "$dir/out")"
	local calls
	calls="$(grep '^CALL ' "$dir/log" || true)"
	local want="CALL issue list --state open --limit 1000 --json number,title
CALL issue create --title $title --label $label --body-file -"
	[ "$calls" = "$want" ] || fail "$name" "called [$calls], want [$want]"
	local text
	for text in "$body_text" stokaro/ptah#2365 https://example.test/run/1; do
		grep -qF -- "$text" "$dir/log" || fail "$name" "the issue leaves out [$text]"
	done
}

# expect_quiet requires success, the outcome in the report, and no gh call.
expect_quiet() {
	local name="$1" outcome="$2"
	shift 2
	cases=$((cases + 1))
	run_watch "$@"
	[ "$status" -eq 0 ] || fail "$name" "exited $status: $(cat "$dir/out")"
	grep -qF -- "Outcome: $outcome" "$dir/out" || fail "$name" "the report leaves out [Outcome: $outcome]: $(cat "$dir/out")"
	[ ! -s "$dir/log" ] || fail "$name" "called gh: $(cat "$dir/log")"
}

# expect_refusal requires a non-zero exit, the reason, and no gh call.
expect_refusal() {
	local name="$1" reason="$2"
	shift 2
	cases=$((cases + 1))
	run_watch "$@"
	[ "$status" -ne 0 ] || fail "$name" "exited zero"
	grep -qF -- "$reason" "$dir/out" || fail "$name" "the output leaves out [$reason]: $(cat "$dir/out")"
	[ ! -s "$dir/log" ] || fail "$name" "called gh: $(cat "$dir/log")"
}

expect_quiet 'an open change at the vendored patch set' 'upstream has not merged the fix' \
	"$pending" "$vendored" open stock "$none" --open-issue
expect_quiet 'the vendored patch set merged' "CL $GO_RUNTIME_PATCH_CHANGE merged as the vendored patch set" \
	"$merged" "$vendored" closed stock "$none" --open-issue
expect_quiet 'an event without --open-issue writes nothing' "$toolchain carries the fix" \
	"$pending" "$vendored" open fixed "$none"

expect_create 'the toolchain carries the fix' "$cleanup" enhancement 'git grep -l go-runtime-patch' \
	"$pending" "$vendored" open fixed "$none"
expect_create 'the toolchain carries the fix of an abandoned change' "$cleanup" enhancement 'git grep -l go-runtime-patch' \
	"$gone" "$vendored" open fixed "$none"
expect_create 'a similar title is another issue' "$cleanup" enhancement 'git grep -l go-runtime-patch' \
	"$pending" "$vendored" open fixed "$similar"
expect_create 'the change has a newer patch set' "$refresh" bug "revisions/$other_revision/patch" \
	"$moved" "$vendored" open stock "$none"
expect_create 'the change merged as another patch set' "$refresh" bug "revisions/$other_revision/patch" \
	"$merged_moved" "$vendored" closed stock "$none"
expect_create 'the change was abandoned' "$abandoned" bug 'Read the upstream thread' \
	"$gone" "$vendored" open stock "$none"
expect_create 'the issue closed without the change' "$closed" bug 'Read the upstream thread' \
	"$pending" "$vendored" closed stock "$none"

cases=$((cases + 1))
run_watch "$pending" "$vendored" open fixed "$listed" --open-issue
[ "$status" -eq 0 ] || fail 'the issue is open already' "exited $status: $(cat "$dir/out")"
[ "$(grep '^CALL ' "$dir/log")" = 'CALL issue list --state open --limit 1000 --json number,title' ] ||
	fail 'the issue is open already' "called [$(cat "$dir/log")], want the listing alone"

cases=$((cases + 1))
: >"$dir/log"
status=0
PATH="$dir/bin:$PATH" STUB_LOG="$dir/log" STUB_ISSUES="$none" STUB_LIST_FAILS=1 \
	GO_RUNTIME_PATCH_CHANGE_JSON="$pending" GO_RUNTIME_PATCH_UPSTREAM_PATCH="$vendored" \
	GO_RUNTIME_PATCH_ISSUE_STATE=open GO_RUNTIME_PATCH_TOOLCHAIN_STACK="$dir/stack-fixed.go" \
	GITHUB_STEP_SUMMARY='' "$here/watch.sh" --open-issue >"$dir/out" 2>&1 || status=$?
[ "$status" -ne 0 ] || fail 'the listing fails' 'exited zero'
! grep -q '^CALL issue create' "$dir/log" || fail 'the listing fails' 'created an issue'

expect_refusal 'the vendored patch is not what Gerrit serves' "is not CL $GO_RUNTIME_PATCH_CHANGE patch set $GO_RUNTIME_PATCH_PATCHSET" \
	"$pending" "$dir/other.patch" open fixed "$none" --open-issue
expect_refusal 'the change record has no status' 'has no status' \
	"$dir/no-status.json" "$vendored" open fixed "$none" --open-issue
expect_refusal 'the change has a status the watch does not know' 'none of NEW, MERGED or ABANDONED' \
	"$unknown" "$vendored" open stock "$none" --open-issue
expect_refusal 'the change record is missing' 'missing.json' \
	"$dir/missing.json" "$vendored" open fixed "$none" --open-issue
expect_refusal 'the issue state is unreadable' 'neither open nor closed' \
	"$pending" "$vendored" locked fixed "$none" --open-issue
expect_refusal 'the toolchain source is empty' 'is empty' \
	"$pending" "$vendored" open empty "$none" --open-issue

if [ "$failures" -ne 0 ]; then
	printf 'watch-selftest: %d failures in %d cases\n' "$failures" "$cases" >&2
	exit 1
fi
printf 'watch-selftest: OK (%d cases)\n' "$cases"

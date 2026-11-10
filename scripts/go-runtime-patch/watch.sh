#!/usr/bin/env bash
# Compares the Go runtime patch the Windows jobs apply with upstream, and opens
# an issue when this repository has to act on the difference.
#
# The patch is an upstream change that has not merged. Every event that ends
# its life here happens upstream, where no pull request in this repository
# shows it:
#
#   - the go.mod toolchain carries the fix: apply.sh already skips patching, so
#     the workaround can be deleted;
#   - the change gains a patch set, or merges as another patch set than the one
#     vendored here: the jobs run code upstream has since changed;
#   - the change is abandoned, or golang/go#81238 closes without it: upstream
#     chose something else.
#
# An open change at the vendored patch set needs nothing, and neither does one
# that merged as that patch set: the patch stays until a toolchain release
# carries the fix, and then the first event applies. Those runs write only the
# report.
#
# Each input is fetched unless the variable naming a fixture is set, which is
# how watch-selftest.sh drives every branch with no network:
#   GO_RUNTIME_PATCH_CHANGE_JSON      Gerrit's change record, )]}' line included
#   GO_RUNTIME_PATCH_UPSTREAM_PATCH   Gerrit's patch for the vendored revision
#   GO_RUNTIME_PATCH_ISSUE_STATE      open or closed
#   GO_RUNTIME_PATCH_TOOLCHAIN_STACK  src/runtime/stack.go at the go.mod toolchain
#
# A fetch that fails stops the script, and so does a vendored patch that differs
# from what Gerrit serves for its revision: reading either as "nothing changed"
# would hide the event this exists to report. An issue is opened once per
# title; when an open issue has the title already, nothing is written.
#
# Usage: watch.sh [--open-issue]. Without the flag nothing is written to GitHub.
# gh reads GH_TOKEN and GH_REPO; RUN_URL, when set, goes into the issue.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/go-runtime-patch/upstream.sh
. "$here/upstream.sh"

die() {
	printf 'watch.sh: %s\n' "$1" >&2
	exit 1
}

open_issue=false
case "${1:-}" in
--open-issue) open_issue=true ;;
'') ;;
*) die "usage: watch.sh [--open-issue]" ;;
esac

toolchain="$(sed -n 's/^toolchain //p' "$here/../../go.mod")"
[ -n "$toolchain" ] || die "go.mod declares no toolchain"

readonly gerrit=https://go-review.googlesource.com
readonly change="CL $GO_RUNTIME_PATCH_CHANGE"
readonly vendored="patch set $GO_RUNTIME_PATCH_PATCHSET (\`${GO_RUNTIME_PATCH_REVISION:0:12}\`)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

if [ -n "${GO_RUNTIME_PATCH_CHANGE_JSON:-}" ]; then
	cp "$GO_RUNTIME_PATCH_CHANGE_JSON" "$work/change.raw"
else
	curl -fsS --retry 3 -o "$work/change.raw" "$gerrit/changes/$GO_RUNTIME_PATCH_CHANGE?o=CURRENT_REVISION"
fi
# Gerrit prefixes every JSON answer with a )]}' line against script inclusion.
tail -n +2 "$work/change.raw" >"$work/change.json"
status="$(jq -er .status "$work/change.json")" || die "Gerrit's record of $change has no status"
current_revision="$(jq -er .current_revision "$work/change.json")" || die "Gerrit's record of $change has no current revision"
current_patchset="$(jq -er '.revisions[.current_revision]._number' "$work/change.json")" || die "Gerrit's record of $change has no patch set number"
case "$status" in
NEW | MERGED | ABANDONED) ;;
*) die "Gerrit reports $change as [$status], which is none of NEW, MERGED or ABANDONED" ;;
esac

if [ -n "${GO_RUNTIME_PATCH_UPSTREAM_PATCH:-}" ]; then
	cp "$GO_RUNTIME_PATCH_UPSTREAM_PATCH" "$work/upstream.patch"
else
	curl -fsS --retry 3 "$gerrit/changes/$GO_RUNTIME_PATCH_CHANGE/revisions/$GO_RUNTIME_PATCH_REVISION/patch" | base64 -d >"$work/upstream.patch"
fi
cmp -s "$here/$GO_RUNTIME_PATCH_FILE" "$work/upstream.patch" ||
	die "scripts/go-runtime-patch/$GO_RUNTIME_PATCH_FILE is not $change $vendored as Gerrit serves it"

if [ -n "${GO_RUNTIME_PATCH_ISSUE_STATE:-}" ]; then
	issue_state="$GO_RUNTIME_PATCH_ISSUE_STATE"
else
	issue_state="$(gh api "repos/golang/go/issues/$GO_RUNTIME_PATCH_ISSUE" --jq .state)"
fi
case "$issue_state" in
open | closed) ;;
*) die "golang/go#$GO_RUNTIME_PATCH_ISSUE reads as [$issue_state], which is neither open nor closed" ;;
esac

if [ -n "${GO_RUNTIME_PATCH_TOOLCHAIN_STACK:-}" ]; then
	cp "$GO_RUNTIME_PATCH_TOOLCHAIN_STACK" "$work/stack.go"
else
	curl -fsS --retry 3 -o "$work/stack.go" "https://raw.githubusercontent.com/golang/go/$toolchain/src/runtime/stack.go"
fi
[ -s "$work/stack.go" ] || die "src/runtime/stack.go at $toolchain is empty"
if grep -qF "$GO_RUNTIME_PATCH_MARKER" "$work/stack.go"; then
	toolchain_fixed=true
else
	toolchain_fixed=false
fi

readonly subject="the Go runtime patch for golang/go#$GO_RUNTIME_PATCH_ISSUE"
readonly context="Context: stokaro/ptah#2365, golang/go#$GO_RUNTIME_PATCH_ISSUE, https://go-review.googlesource.com/c/go/+/$GO_RUNTIME_PATCH_CHANGE."
title=""
label=""
body=""
if [ "$toolchain_fixed" = true ]; then
	outcome="$toolchain carries the fix"
	title="Remove $subject: $outcome"
	label=enhancement
	body="The go.mod toolchain, $toolchain, carries the upstream fix for golang/go#$GO_RUNTIME_PATCH_ISSUE: its \`src/runtime/stack.go\` defines \`${GO_RUNTIME_PATCH_MARKER#func }).\` \`scripts/go-runtime-patch/apply.sh\` already skips patching such a toolchain, so the Windows jobs build with the stock runtime again.

What is left is to delete the workaround: \`git grep -l go-runtime-patch\` lists every file that applies, checks or describes it.

$context"
elif [ "$status" = ABANDONED ]; then
	outcome="$change was abandoned"
	title="Reassess $subject: $outcome"
	label=bug
	body="$change was abandoned upstream. The Windows jobs still apply its $vendored, a fix upstream no longer intends to merge.

Read the upstream thread for what replaced it, then refresh \`scripts/go-runtime-patch\` to that or remove it.

$context"
elif [ "$current_revision" != "$GO_RUNTIME_PATCH_REVISION" ]; then
	outcome="$change is at patch set $current_patchset"
	title="Refresh $subject: $outcome"
	label=bug
	body="$change is $status at patch set $current_patchset (\`$current_revision\`). The Windows jobs apply $vendored, so they run code upstream has since changed.

To refresh, replace the vendored patch with the new patch set:

\`\`\`sh
curl -fsS $gerrit/changes/$GO_RUNTIME_PATCH_CHANGE/revisions/$current_revision/patch | base64 -d >scripts/go-runtime-patch/$GO_RUNTIME_PATCH_FILE
\`\`\`

Then update the patch set and revision in \`scripts/go-runtime-patch/upstream.sh\`, and the marker if the new patch set renames \`${GO_RUNTIME_PATCH_MARKER#func }).\` The Windows jobs prove that the result applies to the go.mod toolchain and that a build links it.

$context"
elif [ "$status" = MERGED ]; then
	outcome="$change merged as the vendored patch set; the Windows jobs keep patching until a toolchain release carries it"
elif [ "$issue_state" = closed ]; then
	outcome="golang/go#$GO_RUNTIME_PATCH_ISSUE closed without $change"
	title="Reassess $subject: $outcome"
	label=bug
	body="golang/go#$GO_RUNTIME_PATCH_ISSUE is closed, while $change is still open at the vendored $vendored. The Windows jobs apply that patch set.

Read the upstream thread for what closed the issue, then refresh \`scripts/go-runtime-patch\` to that or remove it.

$context"
else
	outcome="upstream has not merged the fix; the Windows jobs keep patching"
fi

if [ "$toolchain_fixed" = true ]; then
	toolchain_line="carries the fix"
else
	toolchain_line="does not carry the fix"
fi
report="## Go runtime patch for golang/go#$GO_RUNTIME_PATCH_ISSUE

- Vendored: $change $vendored
- Upstream: $change is $status at patch set $current_patchset (\`${current_revision:0:12}\`)
- golang/go#$GO_RUNTIME_PATCH_ISSUE: $issue_state
- go.mod toolchain $toolchain: $toolchain_line

Outcome: $outcome."
printf '%s\n' "$report"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
	printf '%s\n' "$report" >>"$GITHUB_STEP_SUMMARY"
fi

if [ -z "$title" ] || [ "$open_issue" != true ]; then
	exit 0
fi

# Listing asks the issues themselves, because the search index can lag behind an
# issue just opened. A failed listing stops the script, because reading it as
# "no issue" would open a second one.
open_issues="$(gh issue list --state open --limit 1000 --json number,title)"
existing="$(jq -r --arg title "$title" '[.[] | select(.title == $title) | .number] | min // empty' <<<"$open_issues")"
if [ -n "$existing" ]; then
	echo "issue $existing is open with this title; nothing written"
	exit 0
fi
if [ -n "${RUN_URL:-}" ]; then
	body="$body

Reported by $RUN_URL."
fi
gh issue create --title "$title" --label "$label" --body-file - <<<"$body"

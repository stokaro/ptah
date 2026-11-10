#!/usr/bin/env bash
# Proves apply.sh skips a toolchain that carries the fix and refuses one the
# patch does not fit, without writing to either, and that the go.mod toolchain
# is one of the two outcomes a Windows job can live with.
#
# The patched path itself runs on every Windows job and ends in a verified
# runtime or a failure. What a Windows job cannot show is the skip: it happens
# on the one pull request that raises go.mod to a release with the fix, and a
# skip that never fires reads the same as one that works. So each case builds a
# toolchain tree, runs apply.sh against it, and requires the outcome and an
# unchanged tree.
#
# Run it on Linux or macOS: the last case relies on apply.sh refusing a host
# that is not Windows.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/go-runtime-patch/upstream.sh
. "$here/upstream.sh"
toolchain="$(sed -n 's/^toolchain //p' "$here/../../go.mod")"

dir="$(mktemp -d)"
trap 'rm -rf "$dir"' EXIT

failures=0
cases=0

fail() {
	printf 'apply-selftest: %s: %s\n' "$1" "$2" >&2
	failures=$((failures + 1))
}

tree_sum() {
	(cd "$1" && find . -type f ! -path './bin/*' -exec shasum {} + | sort)
}

# expect runs apply.sh against a tree and requires the exit status, a line of
# its output, and that the tree is byte for byte what it was.
expect() {
	local name="$1" goroot="$2" want_status="$3" want_text="$4"
	cases=$((cases + 1))
	local before status=0
	before="$(tree_sum "$goroot")"
	"$here/apply.sh" "$goroot" >"$dir/out" 2>&1 || status=$?
	[ "$status" -eq "$want_status" ] || fail "$name" "exited $status, want $want_status: $(cat "$dir/out")"
	grep -qF -- "$want_text" "$dir/out" || fail "$name" "the output leaves out [$want_text]: $(cat "$dir/out")"
	[ "$(tree_sum "$goroot")" = "$before" ] || fail "$name" "the tree changed"
}

fake_toolchain() {
	local root="$dir/$1"
	mkdir -p "$root/src/runtime"
	printf 'go9.9.9\ntime 2026-01-01T00:00:00Z\n' >"$root/VERSION"
	printf '%s\n' "$2" >"$root/src/runtime/stack.go"
	printf '%s\n' "$root"
}

fixed="$(fake_toolchain fixed "package runtime

$GO_RUNTIME_PATCH_MARKER n uintptr) {}")"
expect 'a toolchain that carries the fix is skipped' "$fixed" 0 "go9.9.9 already defines ${GO_RUNTIME_PATCH_MARKER#func })"

stale="$(fake_toolchain stale 'package runtime')"
expect 'a toolchain the patch does not fit is refused' "$stale" 1 "does not apply to go9.9.9, and go9.9.9 does not carry the fix"

mkdir -p "$dir/empty"
expect 'a directory that is no toolchain is refused' "$dir/empty" 1 "is not a Go toolchain"

# The files the patch touches, copied out of the go.mod toolchain. On the host
# this runs on, apply.sh reaches the refusal to patch a toolchain that is not
# Windows only if the patch applies; a toolchain that carries the fix is the
# other outcome a Windows job can live with. A patch that does not apply fails
# every Windows job, and this is where a Linux run says so first.
real="$(GOTOOLCHAIN="$toolchain" go env GOROOT)"
current="$dir/current"
mkdir -p "$current/bin"
cp "$real/VERSION" "$current/VERSION"
ln -s "$real/bin/go" "$current/bin/go"
sed -n 's|^diff --git a/\([^ ]*\) b/.*|\1|p' "$here/$GO_RUNTIME_PATCH_FILE" | while read -r file; do
	mkdir -p "$(dirname "$current/$file")"
	cp "$real/$file" "$current/$file"
done
cases=$((cases + 1))
before="$(tree_sum "$current")"
status=0
"$here/apply.sh" "$current" >"$dir/out" 2>&1 || status=$?
if ! grep -qE "refusing to patch a [a-z0-9]+ toolchain|already defines" "$dir/out"; then
	fail "the go.mod toolchain $toolchain" "neither takes the patch nor carries the fix (exit $status): $(cat "$dir/out")"
fi
[ "$(tree_sum "$current")" = "$before" ] || fail "the go.mod toolchain $toolchain" "the tree changed"

if [ "$failures" -ne 0 ]; then
	printf 'apply-selftest: %d failures in %d cases\n' "$failures" "$cases" >&2
	exit 1
fi
printf 'apply-selftest: OK (%d cases)\n' "$cases"

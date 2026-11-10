#!/usr/bin/env bash
# Applies the upstream fix for golang/go#81238 to the Go toolchain a Windows job
# builds with, then proves that a build links the patched runtime.
#
# The go command compiles the standard library from GOROOT/src on demand and
# keys its build cache on file contents, so a patched source tree is all a job
# needs. The prebuilt compiler, linker and vet stay as they are.
#
# The step turns itself off. A toolchain whose runtime already defines the
# function the fix adds is left alone, so the Renovate pull request that raises
# go.mod to a release carrying the fix stops patching without an edit here. Any
# other outcome is a patched runtime confirmed by a program that reads the
# reserve, or a failed job. A patch that no longer applies to a new toolchain
# fails the job: otherwise the crash would come back with nothing saying why.
#
# Usage: apply.sh [GOROOT]. The default is the GOROOT of the go on PATH.
# apply-selftest.sh drives the branches that end before the patch is written.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/go-runtime-patch/upstream.sh
. "$here/upstream.sh"

goroot="${1:-$(go env GOROOT)}"
# Git Bash on a Windows runner: go reports C:\..., and the shell wants /c/...
if command -v cygpath >/dev/null 2>&1; then
	goroot="$(cygpath -u "$goroot")"
fi
patch="$here/$GO_RUNTIME_PATCH_FILE"
stack_go="$goroot/src/runtime/stack.go"
change="CL $GO_RUNTIME_PATCH_CHANGE patch set $GO_RUNTIME_PATCH_PATCHSET"

[ -f "$stack_go" ] || {
	echo "::error::$goroot is not a Go toolchain: $stack_go is missing" >&2
	exit 1
}
version="$(head -n 1 "$goroot/VERSION" 2>/dev/null || echo "the toolchain at $goroot")"

if grep -qF "$GO_RUNTIME_PATCH_MARKER" "$stack_go"; then
	echo "::notice::$version already defines ${GO_RUNTIME_PATCH_MARKER#func }), the fix for golang/go#$GO_RUNTIME_PATCH_ISSUE; not patching. scripts/go-runtime-patch can be removed."
	exit 0
fi

# git apply refuses fuzz, which is the point: a hunk that only nearly matches is
# a runtime nobody reviewed. GOROOT is never inside a repository on a runner, so
# git applies relative to the working directory.
if ! (cd "$goroot" && git apply --check "$patch"); then
	echo "::error::$change does not apply to $version, and $version does not carry the fix for golang/go#$GO_RUNTIME_PATCH_ISSUE. Refresh scripts/go-runtime-patch/$GO_RUNTIME_PATCH_FILE from https://go-review.googlesource.com/c/go/+/$GO_RUNTIME_PATCH_CHANGE (stokaro/ptah#2365)." >&2
	exit 1
fi

# The reserve is set by the Windows osinit; on any other system the patched
# runtime keeps the compile-time floor, and the check below could not tell a
# patched build from a stock one.
hostos="$("$goroot/bin/go" env GOHOSTOS)"
if [ "$hostos" != windows ]; then
	echo "::error::golang/go#$GO_RUNTIME_PATCH_ISSUE is a Windows fault; refusing to patch a $hostos toolchain" >&2
	exit 1
fi

(cd "$goroot" && git apply "$patch")

# The patched runtime defines these variables with their values. Against a stock
# runtime the declarations define zeros of their own, so a zero reserve means
# the build did not use the patched source.
probe="$(mktemp -d)"
trap 'rm -rf "$probe"' EXIT
cat >"$probe/main.go" <<'GO'
package main

import (
	"fmt"
	_ "unsafe" // for go:linkname
)

//go:linkname stackSystemReserve runtime.stackSystemReserve
var stackSystemReserve uintptr

//go:linkname stackMinAlloc runtime.stackMinAlloc
var stackMinAlloc uint32

func main() { fmt.Println(stackSystemReserve, stackMinAlloc) }
GO
probe_out="$(cd "$probe" && "$goroot/bin/go" run -ldflags=-checklinkname=0 main.go)"
read -r reserve min_alloc <<<"$probe_out"
if ! [ "$reserve" -ge 4096 ] 2>/dev/null; then
	echo "::error::applied $change to $version, but a build reports a reserve of [$probe_out]: it did not link the patched runtime" >&2
	exit 1
fi

# 4096 is the floor and what a host without AVX-512 or AMX keeps; an AMX host
# reports about 13.6 KiB and 16 KiB minimum stacks.
report="Patched $version with $change (golang/go#$GO_RUNTIME_PATCH_ISSUE). This host reserves $reserve bytes below each goroutine stack; minimum stack $min_alloc bytes."
echo "::notice::$report"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
	printf '%s\n' "$report" >>"$GITHUB_STEP_SUMMARY"
fi

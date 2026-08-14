#!/usr/bin/env bash

set -euo pipefail

# Run qtlint over every module and every build contour the repository has, with
# one rule set, and report all of them rather than stopping at the first.
#
# The failure this exists to stop: enabling -require-qt-c-receiver in the
# Makefile and in go-lint.yml left three ways to violate it silently, each
# found only after the previous one was fixed.
#
#   1. `go tool qtlint ./...` analyzes the default build contour only. Files
#      behind `//go:build integration` are excluded before the analyzer sees
#      them, so 279 qt.Assert(t, ...) call sites across 23 files under
#      integration/ reported zero findings and exit 0. A gate that inspects
#      nothing passes.
#
#   2. scripts/install-hooks.sh generated a pre-commit hook that ran qtlint
#      without the flag, so the hook accepted what CI then rejected.
#
#   3. Both invocations were still scoped to the root module. testkit/ and
#      examples/orm-loaders/gorm/ are separate modules, so nothing under them
#      was ever analyzed — including testkit/integration/containers_test.go,
#      which is behind the integration tag as well.
#
# Two things make the naive fixes not work, and both cost a debugging cycle:
#
#   - Passing `-tags integration` to the qtlint binary does NOT reach tagged
#     files. The flag is accepted, silently ignored for build constraints, and
#     the command still exits 0 having found nothing. Build tags reach an
#     analyzer through `go vet -tags`, which needs it as a -vettool binary.
#
#   - `go tool qtlint` only works in a module whose go.mod declares the tool.
#     Neither other module does, so the vettool binary is what makes one rule
#     set reachable everywhere.
#
# Modules are discovered rather than listed, so adding one does not silently
# add a fourth blind spot. Every caller — the Makefile target, the go-lint
# workflow step and the managed pre-commit hook — invokes this script, so the
# rule set cannot drift between them again.

RULES=(-require-qt-c-receiver)

# --fix applies the rules' suggested fixes instead of only reporting. The fix
# pass has to cover exactly what the reporting pass covers: `make lint-fix`
# previously ran `go tool qtlint -fix ./...`, which knew neither the rule nor
# the other contours, so it left violations in place and `make lint` then
# failed on them.
FIX=()
case "${1:-}" in
	--fix) FIX=(-fix) ;;
	"") ;;
	*) echo "usage: $(basename "$0") [--fix]" >&2; exit 2 ;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

vettool_dir="$(mktemp -d)"
trap 'rm -rf "$vettool_dir"' EXIT
go build -o "$vettool_dir/qtlint" github.com/go-extras/qtlint/cmd/qtlint

status=0

run_contour() {
	local moddir="$1" label="$2"
	shift 2
	echo "qtlint: ${moddir} (${label})"
	# ${FIX[@]+…} rather than a bare "${FIX[@]}": under set -u, bash 3.2 —
	# still what /bin/bash is on macOS — treats an empty array as unbound and
	# aborts. CI runs bash 5 and would never have shown it.
	if ! (cd "$moddir" && go vet "$@" -vettool="$vettool_dir/qtlint" ${FIX[@]+"${FIX[@]}"} "${RULES[@]}" ./...); then
		status=1
	fi
}

while IFS= read -r modfile; do
	moddir="$(dirname "${modfile#./}")"
	run_contour "$moddir" "default contour"
	run_contour "$moddir" "integration contour" -tags integration
done < <(find . -name go.mod -not -path './.git/*' -not -path '*/testdata/*' | sort)

if [ "$status" -ne 0 ]; then
	echo "qtlint: FAILED" >&2
	exit 1
fi

echo "qtlint: OK (every module, default and integration contours)"

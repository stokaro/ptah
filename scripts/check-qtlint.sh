#!/usr/bin/env bash

set -euo pipefail

# Run qtlint over every build contour the repository has, not just the default
# one, and with the same rule set everywhere.
#
# The failure this exists to stop: enabling -require-qt-c-receiver in the
# Makefile and in go-lint.yml left two ways to violate it silently.
#
#   1. `go tool qtlint ./...` analyzes the default build contour only. Files
#      behind `//go:build integration` are excluded before the analyzer ever
#      sees them, so 279 qt.Assert(t, ...) call sites across 23 files under
#      integration/ reported zero findings and exit 0. A gate that inspects
#      nothing passes.
#
#   2. scripts/install-hooks.sh generated a pre-commit hook that ran qtlint
#      without the flag, so the hook accepted what CI then rejected.
#
# Passing `-tags integration` to the qtlint binary does NOT fix the first one:
# the flag is accepted, silently ignored for build constraints, and the command
# still exits 0 having found nothing. Build tags reach an analyzer through
# `go vet -tags`, which needs the analyzer as a -vettool binary.
#
# Every caller — the Makefile target, the go-lint workflow and the managed
# pre-commit hook — invokes this script, so the rule set cannot drift between
# them again.

RULES=(-require-qt-c-receiver)

echo "qtlint: default contour"
go tool qtlint "${RULES[@]}" ./...

# `go vet -vettool` requires a built binary; `go tool` cannot be handed to it.
vettool_dir="$(mktemp -d)"
trap 'rm -rf "$vettool_dir"' EXIT
go build -o "$vettool_dir/qtlint" github.com/go-extras/qtlint/cmd/qtlint

echo "qtlint: integration contour"
go vet -tags integration -vettool="$vettool_dir/qtlint" "${RULES[@]}" ./integration/...

echo "qtlint: OK (default and integration contours)"

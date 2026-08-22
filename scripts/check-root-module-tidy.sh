#!/usr/bin/env bash
# Refuses a root go.mod that `go mod tidy` would change.
#
# The repository already checks this for testkit, in .github/workflows/
# testkit.yml, and that check does `cd testkit`. The root module had none, so an
# untidy root go.mod was the one shape that reached master with every job green.
#
# What it looks like: a Renovate major bump on a Go module renames the import
# path, and `/v2` and `/v3` are different modules rather than two versions of
# one. Requiring the new one adds it beside the old rather than replacing it, so
# the build keeps linking v2, nothing imports v3, and `go mod tidy` deletes the
# line. Measured on stokaro/ptah#1888: go.mod required both go-ora/v2 v2.9.0 and
# go-ora/v3 v3.0.1, `go list -deps` resolved v2, no Go file named v3, and every
# check passed because the tree still compiled.
#
# `go mod tidy -diff` rather than a tidy followed by `git diff`: it writes
# nothing, prints the changes it would make, and exits non-zero by itself, so
# the check does not depend on a clean working tree and cannot repair the
# staleness it is supposed to report.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if go mod tidy -diff; then
  echo "check-root-module-tidy: OK (the root go.mod and go.sum are what tidy would write)"
  exit 0
fi

echo "::error file=go.mod::the root module is untidy. Run 'go mod tidy' and commit the result." >&2
echo "" >&2
echo "A require nothing imports is usually a major-version bump that renamed the" >&2
echo "import path without moving the imports: /v2 and /v3 are separate modules," >&2
echo "so the old one is still what the build links." >&2
exit 1

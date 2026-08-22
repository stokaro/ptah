#!/usr/bin/env bash
# Proves check-root-module-tidy.sh fails on the shape it exists to catch.
#
# The guard passes on the tree as it stands, and a guard whose only observed
# result is "pass" is indistinguishable from one that examines nothing. This
# reconstructs the defect in a throwaway module and requires a refusal, then
# repairs it and requires acceptance.
#
# The defect is reconstructed rather than described: a require on a module no
# file imports, which is what a major-version bump leaves behind when it renames
# the import path and moves no imports.
#
# The guard is invoked from inside the throwaway rather than by path, because it
# resolves its own root with `git rev-parse --show-toplevel` -- which reads the
# working directory, not the script's location. Calling it by path from here ran
# it against this repository, which is tidy, and the selftest passed while
# examining nothing. That is the failure this file exists to make impossible,
# arriving in the file itself.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-root-tidy-selftest.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

mkdir -p "$work_dir/scripts"
git -C "$work_dir" init --quiet
cp "$repo_root/scripts/check-root-module-tidy.sh" "$work_dir/scripts/"

cat >"$work_dir/main.go" <<'GO'
package main

func main() {}
GO

# The defect: a require nothing imports. golang.org/x/text is chosen because it
# is in the module cache of any tree that built this repository, so the guard is
# offline here for the same reason the real one is.
cat >"$work_dir/go.mod" <<GO
module example.local/roottidy

go 1.24

require golang.org/x/text v0.3.8
GO

set +e
(cd "$work_dir" && GOFLAGS=-mod=mod ./scripts/check-root-module-tidy.sh) >"$work_dir/untidy.log" 2>&1
untidy_status=$?
set -e

if [ "$untidy_status" -eq 0 ]; then
  echo "check-root-module-tidy-selftest: FAILED -- an unimported require was accepted" >&2
  cat "$work_dir/untidy.log" >&2
  exit 1
fi

# Repaired: the same module with nothing spurious in it.
cat >"$work_dir/go.mod" <<GO
module example.local/roottidy

go 1.24
GO

set +e
(cd "$work_dir" && GOFLAGS=-mod=mod ./scripts/check-root-module-tidy.sh) >"$work_dir/tidy.log" 2>&1
tidy_status=$?
set -e

if [ "$tidy_status" -ne 0 ]; then
  echo "check-root-module-tidy-selftest: FAILED -- a tidy module was refused" >&2
  cat "$work_dir/tidy.log" >&2
  exit 1
fi

echo "check-root-module-tidy-selftest: OK (refuses an unimported require, accepts a tidy module)"

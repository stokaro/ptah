#!/usr/bin/env bash

set -euo pipefail

# Prints the directory of every Go module tracked in this repository, one per
# line, root first. It is the single answer to "which modules does a
# repository-wide tool have to visit", so that a tool covers a module because
# the module exists rather than because someone remembered to add it.
#
# The list comes from git rather than from a filesystem walk, for the reason
# scripts/check-test-style.sh already documents: the root of a linked git
# worktree is an ordinary directory whose `.git` is a regular file, so a walk
# descends into every checkout parked under this one and reports modules that
# belong to a different tree. `git ls-files` cannot leave the working tree.
#
# This exists because the three repository-wide tools disagreed. qtlint took
# -multi-module and discovered every module; nolintguard named all three by
# hand in six places; golangci-lint ran from the repository root and therefore
# linted the root module only. Measured: a file planted in testkit/ with two
# `unused` findings was reported by `golangci-lint run ./...` inside testkit and
# not reported at all from the root, so testkit -- a published module -- was
# unlinted, and so was examples/orm-loaders/gorm.

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if [[ ${1:-} == "--selftest" ]]; then
	modules="$(git ls-files 'go.mod' '*/go.mod')"
	if [[ -z $modules ]]; then
		printf '%s: git tracks no go.mod at all; refusing to report an empty module list\n' "$0" >&2
		exit 1
	fi
	if ! printf '%s\n' "$modules" | grep -qx 'go.mod'; then
		printf '%s: the root go.mod is not tracked; the list would omit the main module\n' "$0" >&2
		exit 1
	fi
	while read -r module; do
		if [[ ! -f "$module/go.mod" ]]; then
			printf '%s: reported %s, which has no go.mod on disk\n' "$0" "$module" >&2
			exit 1
		fi
	done < <("$0")
	printf '%s: OK (%d modules)\n' "$0" "$("$0" | wc -l | tr -d ' ')"
	exit 0
fi

# `dirname go.mod` is `.`, which sorts before every subdirectory, so the main
# module is first without special-casing it.
git ls-files 'go.mod' '*/go.mod' | while read -r manifest; do
	dirname "$manifest"
done | sort

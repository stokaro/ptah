#!/usr/bin/env bash

set -euo pipefail

# Every Go module in this repository must be linted by CI, and this checks that
# the workflow says so for each one.
#
# The Makefile discovers its modules (see scripts/list-go-modules.sh), so
# `make lint` cannot miss one. The golangci-lint step cannot discover: the
# action lints one directory per invocation, so the workflow names the modules
# it visits. A hand-written list is exactly what went stale before -- until this
# check landed, golangci-lint ran from the repository root and therefore linted
# only the root module, leaving testkit (a published module) and
# examples/orm-loaders/gorm unlinted, while nolintguard named all three by hand
# in six places and qtlint discovered all three by itself. Three tools, three
# different answers to the same question.
#
# A new module therefore fails here until the workflow visits it, which is the
# moment the decision is cheap.

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

workflow=".github/workflows/go-lint.yml"

if [[ ! -f $workflow ]]; then
	printf 'go module lint coverage: %s not found\n' "$workflow" >&2
	exit 1
fi

modules="$(scripts/list-go-modules.sh)"
if [[ -z $modules ]]; then
	printf 'go module lint coverage: no modules discovered; refusing to report a vacuous pass\n' >&2
	exit 1
fi

# The root module is linted by the invocation that names no working-directory,
# so counting those counts the jobs that lint at all -- two today, Ubuntu and
# Windows. Every other module then has to appear once per such job.
#
# Counting rather than merely finding matters: with `grep -q` a module dropped
# from one job stays green on the strength of the other, which is the same
# "somewhere in the file" reasoning that let the modules go uncovered in the
# first place. Removing `working-directory: testkit` from one job was measured
# against both spellings; only the count reports it.
invocations="$(grep -c 'uses: golangci/golangci-lint-action' "$workflow" || true)"
scoped="$(grep -c 'working-directory:' "$workflow" || true)"
jobs=$((invocations - scoped))

if [[ $jobs -le 0 ]]; then
	printf 'go module lint coverage: %s runs golangci-lint in no job at all\n' "$workflow" >&2
	exit 1
fi

missing=0
while read -r module; do
	if [[ $module == "." ]]; then
		continue
	fi
	found="$(grep -cF "working-directory: $module" "$workflow" || true)"
	if [[ $found -ne $jobs ]]; then
		printf 'go module lint coverage: %s is linted by %d of the %d golangci-lint jobs in %s\n' \
			"$module" "$found" "$jobs" "$workflow" >&2
		printf '  every job needs a step with `working-directory: %s`\n' "$module" >&2
		missing=1
	fi
done <<<"$modules"

if [[ $missing -ne 0 ]]; then
	exit 1
fi

printf 'go module lint coverage: OK (%d modules across %d jobs)\n' \
	"$(printf '%s\n' "$modules" | wc -l | tr -d ' ')" "$jobs"

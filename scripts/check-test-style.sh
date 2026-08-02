#!/usr/bin/env sh
set -eu

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# git grep rather than rg: when ripgrep is absent the command exits 127, the
# `if` reads that as "no matches", and `set -e` does not fire inside a condition
# -- so this gate reported success without ever running. It did exactly that on
# CI, which has no ripgrep. git grep is always present here and searches tracked
# files only, which also keeps stray worktrees out of the result.
if git grep -nE 'github\.com/stretchr/testify|\b(assert|require)\.' -- '*.go'; then
	echo "teststyle: testify/assert/require usage is prohibited; use quicktest as qt instead" >&2
	exit 1
fi

go_cache="${GOCACHE:-$(go env GOCACHE)}"
if ! mkdir -p "$go_cache" 2>/dev/null || [ ! -w "$go_cache" ]; then
	GOCACHE="${TMPDIR:-/tmp}/ptah-go-cache"
	export GOCACHE
	mkdir -p "$GOCACHE"
fi

GOWORK=off go tool teststyle -baseline .teststyle-baseline.json -root .

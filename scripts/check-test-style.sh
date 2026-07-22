#!/usr/bin/env sh
set -eu

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

go_cache="${GOCACHE:-$(go env GOCACHE)}"
if ! mkdir -p "$go_cache" 2>/dev/null || [ ! -w "$go_cache" ]; then
	GOCACHE="${TMPDIR:-/tmp}/ptah-go-cache"
	export GOCACHE
	mkdir -p "$GOCACHE"
fi

GOWORK=off go run ./internal/tools/teststyle -baseline .teststyle-baseline.json -root .

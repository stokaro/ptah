#!/usr/bin/env sh
set -eu

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if rg -n 'github\.com/stretchr/testify|\b(assert|require)\.' --glob '*.go' .; then
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

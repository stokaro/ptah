#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source_root="$(cd "$script_dir/.." && pwd)"

if [[ "${1:-}" == "--selftest" ]]; then
	(
		cd "$source_root"
		go test ./cmd/internal/goversionguard -count=1
	)
	echo "check-go-version-consistency.sh --selftest: OK"
	exit 0
fi

repo_root="${1:-$source_root}"
cd "$repo_root"

expected_version="$(awk '$1 == "go" { print $2; exit }' go.mod)"
if [[ -z "$expected_version" ]]; then
	echo "error: go.mod has no Go version directive" >&2
	exit 1
fi

failures=0

check_version() {
	local source="$1"
	local actual_version="$2"

	if [[ "$actual_version" == "$expected_version" ]]; then
		printf '%s: Go %s\n' "$source" "$actual_version"
		return
	fi

	printf 'error: %s uses Go %s; expected %s from go.mod\n' \
		"$source" "${actual_version:-<missing>}" "$expected_version" >&2
	failures=$((failures + 1))
}

read_golangci_version() {
	awk '$1 == "go:" { gsub(/"/, "", $2); print $2; exit }' "$1"
}

read_documented_action_default() {
	awk -F '|' '
		$2 ~ /`go-version`/ {
			value = $3
			gsub(/[ `]/, "", value)
			print value
			exit
		}
	' "$1"
}

check_version ".golangci.yml" "$(read_golangci_version .golangci.yml)"

for docs_file in docs/github_action.md docs/site/src/content/docs/testing/ci.md; do
	check_version "$docs_file go-version default" \
		"$(read_documented_action_default "$docs_file")"
done

if ((failures > 0)); then
	printf 'error: found %d inconsistent Go version pin(s)\n' "$failures" >&2
	exit 1
fi

(
	cd "$source_root"
	go run ./cmd/internal/goversionguard -root "$repo_root" -version "$expected_version"
)

printf 'All canonical Go version pins match %s.\n' "$expected_version"

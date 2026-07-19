#!/usr/bin/env bash
set -euo pipefail

export GOWORK=off

snapshot="docs/public_api.snapshot"
update=0
if [[ "${1:-}" == "--update" ]]; then
	update=1
fi

tmp="$(mktemp)"
packages="$(mktemp)"
trap 'rm -f "$tmp" "$packages"' EXIT

grep -Eo '`github\.com/stokaro/ptah[^`]+`' docs/public_api.md |
	tr -d '`' |
	sort -u >"$packages"

while IFS= read -r package_path; do
	printf '## %s\n\n' "$package_path" >>"$tmp"
	go doc -short "$package_path" | sed 's/[[:space:]]*$//' >>"$tmp"
	printf '\n' >>"$tmp"
done <"$packages"

if [[ "$update" -eq 1 ]]; then
	cp "$tmp" "$snapshot"
	exit 0
fi

diff -u "$snapshot" "$tmp"

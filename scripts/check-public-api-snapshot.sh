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
	doc="$(go doc -short "$package_path")"
	printf '## %s\n\n' "$package_path" >>"$tmp"
	printf '%s\n' "$doc" | expand -t 4 | sed 's/[[:space:]]*$//' >>"$tmp"
	printf '\n' >>"$tmp"

	printf '%s\n' "$doc" |
		sed -n -E 's/^type ([[:upper:]][[:alnum:]_]*) interface.*/\1/p' |
		while IFS= read -r type_name; do
			printf '### %s.%s\n\n' "$package_path" "$type_name" >>"$tmp"
			go doc "$package_path.$type_name" | expand -t 4 | sed 's/[[:space:]]*$//' >>"$tmp"
			printf '\n' >>"$tmp"
		done
done <"$packages"

if [[ "$update" -eq 1 ]]; then
	cp "$tmp" "$snapshot"
	exit 0
fi

diff -u "$snapshot" "$tmp"

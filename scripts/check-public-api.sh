#!/usr/bin/env bash
set -euo pipefail

export GOWORK=off

module_path="$(go list -m -f '{{.Path}}')"
allowlist="$(mktemp)"
packages="$(mktemp)"
trap 'rm -f "$allowlist" "$packages"' EXIT

grep -Eo '`github\.com/stokaro/ptah[^`]+`' docs/public_api.md |
	tr -d '`' |
	sort -u >"$allowlist"

go list -f '{{.ImportPath}}|{{.Name}}' ./... >"$packages"

missing=0
while IFS='|' read -r import_path package_name; do
	case "$import_path" in
		"$module_path"/cmd | "$module_path"/cmd/*) continue ;;
		"$module_path"/examples | "$module_path"/examples/*) continue ;;
		"$module_path"/integration | "$module_path"/integration/*) continue ;;
		"$module_path"/stubs) continue ;;
		"$module_path"/internal | "$module_path"/internal/*) continue ;;
		"$module_path"/*/internal/*) continue ;;
		"$module_path"/*/testutil) continue ;;
		"$module_path"/*/mocks) continue ;;
	esac
	if [[ "$package_name" == "main" ]]; then
		continue
	fi
	if ! grep -Fxq "$import_path" "$allowlist"; then
		printf 'undocumented public package: %s\n' "$import_path" >&2
		missing=1
	fi
done <"$packages"

if [[ "$missing" -ne 0 ]]; then
	printf 'Add the package to docs/public_api.md or move it behind an internal/ boundary.\n' >&2
	exit 1
fi

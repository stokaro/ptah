#!/usr/bin/env bash
set -euo pipefail

export GOWORK=off

module_path="$(go list -m -f '{{.Path}}')"
allowlist="$(mktemp)"
packages="$(mktemp)"
trap 'rm -f "$allowlist" "$packages"' EXIT

# List items only. A backticked package path in a prose paragraph is a
# mention, not a listing, and must not join the allowlist.
#
# The recognition lives in internal/featureinventory, not here. This gate and
# the feature inventory need the identical set, and two implementations of one
# rule is what AGENTS.md's "recognition that spans two functions belongs to one
# of them" forbids -- with a quiet failure mode: a pattern that drifts by one
# character produces a SMALLER allowlist, and a smaller allowlist reports fewer
# undocumented packages rather than an error. The tool refuses a vacuous ledger
# itself, so nothing below re-checks for an empty file.
#
# PTAH_FEATURE_INVENTORY lets check-public-api-selftest.sh aim this gate at a
# prebuilt binary, so the fixture can be a throwaway module that has no
# internal/cmd of its own. It is the same seam check-compose-image-pins.sh
# opens for the same reason (stokaro/ptah#2509).
if [[ -n ${PTAH_FEATURE_INVENTORY:-} ]]; then
	"$PTAH_FEATURE_INVENTORY" --list-ledger | sort -u >"$allowlist"
else
	go run ./internal/cmd/featureinventory --list-ledger | sort -u >"$allowlist"
fi

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

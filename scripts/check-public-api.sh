#!/usr/bin/env bash
# Fails when a library package is importable from outside this module and
# docs/public_api.md classifies it under neither category.
#
# "Library package" is read out of package metadata rather than out of a path
# pattern. `go list` reports a directory holding only `_test.go` files, and such
# a directory publishes no import path an embedder can use; so does a `main`
# package. Both are outside the surface because of what they contain, which is
# a property this can measure, rather than because of where they sit, which is
# a guess that goes wrong as soon as a tree is reorganized (stokaro/ptah#2974).
set -euo pipefail

export GOWORK=off

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

module_path="$(go list -m -f '{{.Path}}')"
allowlist="$(mktemp)"
packages="$(mktemp)"
trap 'rm -f "$allowlist" "$packages"' EXIT

# Both categories. The boundary question is whether a public import path may
# exist at all; whether it carries a compatibility guarantee is the separate
# question every other public-API check asks, and those read --list-ledger.
#
# The recognition lives in internal/featureinventory, not here. This gate and
# the feature inventory need the identical classification, and two
# implementations of one rule is what AGENTS.md's "recognition that spans two
# functions belongs to one of them" forbids -- with a quiet failure mode: a
# pattern that drifts by one character produces a SMALLER allowlist, and a
# smaller allowlist reports fewer undocumented packages rather than an error.
# The tool refuses a vacuous ledger itself, so nothing below re-checks for an
# empty file.
#
# PTAH_FEATURE_INVENTORY lets check-public-api-selftest.sh aim this gate at a
# prebuilt binary, so the fixture can be a throwaway module that has no
# internal/cmd of its own. It is the same seam check-compose-image-pins.sh
# opens for the same reason (stokaro/ptah#2509).
if [[ -n ${PTAH_FEATURE_INVENTORY:-} ]]; then
	"$PTAH_FEATURE_INVENTORY" --list-boundary | sort -u >"$allowlist"
else
	go run ./internal/cmd/featureinventory --list-boundary | sort -u >"$allowlist"
fi

# The build configurations this census covers. A `go list` on the host answers
# for the host, and a package selected by a build constraint no other
# configuration selects would never be seen: `windows` and the `integration`
# tag each add packages this repository really has. GOARCH stays fixed because
# nothing here is selected by word size.
#
# This is the stated coverage, not a claim of completeness. A package gated on
# a tag absent from this list is outside what the gate measures, which is why
# adding such a tag means adding it here.
build_configurations=(
	"linux "
	"darwin "
	"windows "
	"linux integration"
	"darwin integration"
	"windows integration"
)

# A package-loading failure is fatal. `go list` without -e exits non-zero when
# it cannot load a package, and `set -e` carries that out of the loop: a broken
# tree must not report an empty surface, which reads exactly like a clean one.
for configuration in "${build_configurations[@]}"; do
	read -r goos tags <<<"$configuration"
	GOOS="$goos" GOARCH=amd64 go list -tags "$tags" \
		-f '{{.ImportPath}}|{{.Name}}|{{len .GoFiles}}|{{len .CgoFiles}}' ./... >>"$packages"
done
sort -u -o "$packages" "$packages"

if [[ ! -s "$packages" ]]; then
	printf '%s: go list reported no packages at all; refusing to report a vacuous surface\n' "$0" >&2
	exit 1
fi

# is_internal answers whether an import path crosses a Go internal boundary.
#
# The comparison is on a whole path segment. A prefix match calls
# `ptah.run/internalized` internal, and the nested pattern this replaces
# (`*/internal/*`) missed a package sitting directly in `core/internal` while
# matching its descendants -- so the boundary held for `core/internal/detail`
# and not for the package above it.
is_internal() {
	case "/$1/" in
		*/internal/*) return 0 ;;
	esac
	return 1
}

# Named exemptions, deleted by the change that internalizes the subtree each one
# names (stokaro/ptah#2974). An exemption is not a boundary: it keeps a package
# out of docs/public_api.md while Go goes on publishing it, so each is a
# recorded debt with an owner rather than a policy. None may be added.
#
# `integration` covers both the fixture series and the reference helper beside
# the suites, and comes off only when both have moved.
is_exempt() {
	case "$1" in
		"$module_path"/cmd | "$module_path"/cmd/*) return 0 ;;
		"$module_path"/integration | "$module_path"/integration/*) return 0 ;;
		"$module_path"/stubs) return 0 ;;
	esac
	return 1
}

missing=0
libraries=0
while IFS='|' read -r import_path package_name go_files cgo_files; do
	if [[ "$package_name" == "main" ]]; then
		continue
	fi
	# A directory whose only Go files are tests publishes nothing importable.
	if [[ $((go_files + cgo_files)) -eq 0 ]]; then
		continue
	fi
	if is_internal "$import_path"; then
		continue
	fi
	libraries=$((libraries + 1))
	if is_exempt "$import_path"; then
		continue
	fi
	if ! grep -Fxq "$import_path" "$allowlist"; then
		printf 'unclassified public package: %s\n' "$import_path" >&2
		missing=1
	fi
done <"$packages"

# A corpus floor. Every rule above narrows the set, and a narrowing that went
# too far reports zero findings, which is the same output as a clean tree.
if [[ "$libraries" -eq 0 ]]; then
	printf '%s: classified no library packages at all; the filters cannot all be right\n' "$0" >&2
	exit 1
fi

if [[ "$missing" -ne 0 ]]; then
	printf 'List the package in docs/public_api.md -- under Stable Embedder API if it is an\n' >&2
	printf 'embedder contract, under Documentation-Only Packages if documentation reaches it\n' >&2
	printf 'and it carries no guarantee -- or move it behind an internal/ boundary.\n' >&2
	exit 1
fi

printf '%s: OK (%d importable library packages, all classified)\n' "$0" "$libraries"

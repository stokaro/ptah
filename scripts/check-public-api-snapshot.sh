#!/usr/bin/env bash
set -euo pipefail

export GOWORK=off

# emit_package_snapshot writes the snapshot fragment for a single package to
# stdout. The fragment has two parts:
#
#   1. A "## <package>" header followed by the raw `go doc -short` listing, which
#      records the package-level API surface (consts, funcs, and the one-line
#      form of every type).
#   2. A "### <package>.<Type>" section for EVERY exported named type — struct,
#      interface, alias, map, func type, any `type X ...` where X is exported —
#      followed by the full `go doc <package>.<Type>` output. This is what makes
#      the guard sensitive to changes in exported struct fields and in methods on
#      concrete named types, not just interface method sets.
#
# The exported type names are sorted (LC_ALL=C, byte order) before their blocks
# are emitted so the fragment is reproducible regardless of the order `go doc`
# happens to list types across a package's source files.
emit_package_snapshot() {
	local package_path="$1"
	local doc
	doc="$(go doc -short "$package_path")"

	printf '## %s\n\n' "$package_path"
	printf '%s\n' "$doc" | expand -t 4 | sed 's/[[:space:]]*$//'
	printf '\n'

	printf '%s\n' "$doc" |
		sed -n -E 's/^type ([[:upper:]][[:alnum:]_]*).*/\1/p' |
		LC_ALL=C sort -u |
		while IFS= read -r type_name; do
			printf '### %s.%s\n\n' "$package_path" "$type_name"
			go doc "$package_path" "$type_name" | expand -t 4 | sed 's/[[:space:]]*$//'
			printf '\n'
		done
}

# Internal mode used by the guard self-test (internal/apiguard): emit the
# fragment for a single package to stdout without touching the snapshot file or
# reading docs/public_api.md. This keeps the per-package generation logic in one
# place so the test exercises exactly what the snapshot is built from.
if [[ "${1:-}" == "--emit-package" ]]; then
	if [[ "$#" -ne 2 ]]; then
		printf 'usage: %s --emit-package <package-import-path>\n' "$0" >&2
		exit 2
	fi
	emit_package_snapshot "$2"
	exit 0
fi

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
	emit_package_snapshot "$package_path" >>"$tmp"
done <"$packages"

if [[ "$update" -eq 1 ]]; then
	cp "$tmp" "$snapshot"
	exit 0
fi

diff -u "$snapshot" "$tmp"

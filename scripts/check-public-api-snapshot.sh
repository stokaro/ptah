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

# list_ledger_packages emits the sorted package set docs/public_api.md lists,
# one import path per line. Only list items count: the ledger's entries are
# bullets, and a backticked path inside a prose paragraph is a mention, not a
# listing, so it must not join the set.
#
# The recognition itself lives in internal/featureinventory and this forwards to
# it. Three gates used to scrape the ledger with a pattern each, which is what
# AGENTS.md's "recognition that spans two functions belongs to one of them"
# forbids -- with the quiet failure mode that rule describes: a pattern that
# drifts by one character produces a SMALLER set, and a smaller set reports
# FEWER undocumented packages and FEWER incompatible-change findings rather than
# an error. The module path is read from go.mod there, so it still moves with
# the module rather than being restated.
#
# The module directory is passed explicitly because internal/apiguard runs this
# mode against a throwaway fixture module: the answer has to be about the
# caller's directory, not about this one. `go -C` is what lets the command be
# built from this repository while the ledger being read belongs to another.
list_ledger_packages() {
	local repo_root module_dir
	repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
	module_dir="$(pwd)"
	go -C "$repo_root" run ./internal/cmd/featureinventory --list-ledger --root "$module_dir"
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

# Internal mode used by the docs-sync gate (check-public-api-docs-sync.sh) and
# the guard self-test (internal/apiguard): print the set of packages the ledger
# lists and nothing else, so every consumer reads the ledger through this one
# scrape.
if [[ "${1:-}" == "--list-packages" ]]; then
	list_ledger_packages
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

# The floor that stops an empty ledger from producing an empty snapshot, which
# would match an empty recorded one and exit 0, is inside the scrape:
# `featureinventory --list-ledger` refuses rather than printing nothing. A second
# `[[ ! -s ]]` here would be a branch nothing can reach and no fixture can drive,
# which is a line that reads as coverage while measuring nothing.
list_ledger_packages >"$packages"

while IFS= read -r package_path; do
	emit_package_snapshot "$package_path" >>"$tmp"
done <"$packages"

if [[ "$update" -eq 1 ]]; then
	cp "$tmp" "$snapshot"
	exit 0
fi

diff -u "$snapshot" "$tmp"

#!/usr/bin/env bash
# Fails when the site's stable-packages table and the public API ledger name
# different package sets.
#
# docs/public_api.md is the enforced ledger of the public Go surface, but the
# site page docs/site/src/content/docs/extend/public-api.md carries its own
# hand-written table of the same packages. Nothing tied the two together, and
# the table fell six packages behind the ledger before anyone noticed
# (stokaro/ptah#2246). This gate compares the two sets and reports the
# difference in both directions.
#
# The ledger side is read through `list-public-api-packages.sh`, the same
# command the API gates run, so this script cannot drift from what those gates
# enforce.
set -euo pipefail

export GOWORK=off

script_dir="$(cd "$(dirname "$0")" && pwd)"
site_page="docs/site/src/content/docs/extend/public-api.md"

module_path="$(go list -m -f '{{.Path}}')"
ledger_packages="$(mktemp)"
site_packages="$(mktemp)"
trap 'rm -f "$ledger_packages" "$site_packages"' EXIT

# The ledger lists full import paths; the site table lists them without the
# module prefix. Strip the prefix so the two sets compare in one spelling.
"$script_dir/list-public-api-packages.sh" |
	sed "s|^${module_path}/||" |
	sort -u >"$ledger_packages"

# Only the stable-packages table counts: the page's other tables name scripts,
# not packages, and prose mentions are not listings.
awk '/^## Stable packages$/ { in_section = 1; next } /^## / { in_section = 0 } in_section' "$site_page" |
	sed -n -E 's/^\| `([^`]+)` \|.*/\1/p' |
	sort -u >"$site_packages"

if [[ ! -s "$ledger_packages" ]]; then
	printf '%s: found no packages in docs/public_api.md; refusing to report a vacuous pass\n' "$0" >&2
	exit 1
fi
if [[ ! -s "$site_packages" ]]; then
	printf '%s: found no packages in the stable-packages table of %s; refusing to report a vacuous pass\n' \
		"$0" "$site_page" >&2
	exit 1
fi

status=0

missing_from_site="$(comm -23 "$ledger_packages" "$site_packages")"
if [[ -n "$missing_from_site" ]]; then
	printf 'listed in docs/public_api.md but missing from the stable-packages table of %s:\n' "$site_page" >&2
	printf '%s\n' "$missing_from_site" | sed 's/^/  /' >&2
	status=1
fi

missing_from_ledger="$(comm -13 "$ledger_packages" "$site_packages")"
if [[ -n "$missing_from_ledger" ]]; then
	printf 'in the stable-packages table of %s but not listed in docs/public_api.md:\n' "$site_page" >&2
	printf '%s\n' "$missing_from_ledger" | sed 's/^/  /' >&2
	status=1
fi

if [[ "$status" -ne 0 ]]; then
	printf 'The ledger and the site table must name the same packages; update whichever is behind.\n' >&2
fi

exit "$status"

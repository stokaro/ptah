#!/usr/bin/env bash
# Checks the feature inventory's page columns against the documents themselves.
#
# The other five feature-coverage gates read `Feature ID` and `Public surface`,
# the documents, or a generated block. None of them read `Canonical page` or
# `Example`, which is the register's central promise: which page owns each
# surface. Five rows named a page that did not carry their surface at all while
# every other number in the file reproduced exactly, and every gate was green.
#
# Three rules: the page a row names exists; a row claiming a runnable example
# names a document with at least one fenced code block; and where the row's
# surface carries a command path or a `PTAH_*` variable -- the two spellings a
# page cannot paraphrase -- the named document contains one of the row's tokens.
#
# `--selftest` runs the fixtures instead: each one plants the defect this gate
# exists to catch, and each one requires the analyzer to name it. A gate nobody
# has watched fail is not a gate.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
# shellcheck source=scripts/lib/featureinventory.sh
. scripts/lib/featureinventory.sh

case "${1-}" in
"")
	run_feature_inventory_gate "check-inventory-pages" 'TestInventoryPages_' \
		"every page docs/feature-inventory.md names exists, and carries the surface the row claims it owns"
	;;
--selftest)
	run_feature_inventory_gate "check-inventory-pages --selftest" 'TestInventoryPagesSelftest_' \
		"the fixtures each plant a page that does not resolve, an example on a document with no fenced block, or a page that never names the surface"
	;;
*)
	feature_inventory_usage "$0"
	;;
esac

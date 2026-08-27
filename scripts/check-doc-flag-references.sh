#!/usr/bin/env bash
# Checks documented invocations against the flag set of the command they name.
#
# Scoped to the command path, never to the whole tree. Over the tracked
# documents, 247 `--flag` mentions name a flag no tree registers, and every one
# is correct and about another program: `--rm` is docker, `--scenarios` is the
# integration runner, `--selftest` is these scripts.
# docs/site/scripts/check-matrix-flag-names.mjs states the same limit from the
# other side, and stokaro/ptah#1924 records what it costs.
#
# The flag set comes from the cobra tree rather than from `--help`: seven
# ptah-compat commands print no flag block at all while registering four flags
# each, so a census built on help under-reports them.
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
	run_feature_inventory_gate "check-doc-flag-references" 'TestDocFlagReferences_' \
		"no documented invocation passes a flag the command path it names does not register"
	;;
--selftest)
	run_feature_inventory_gate "check-doc-flag-references --selftest" 'TestDocFlagReferencesSelftest_' \
		"the fixtures each plant a flag that does not exist, or one of the flags the scan must not read"
	;;
*)
	feature_inventory_usage "$0"
	;;
esac

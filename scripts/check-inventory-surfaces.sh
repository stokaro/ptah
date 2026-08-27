#!/usr/bin/env bash
# Checks the feature inventory against the surfaces that are not commands.
#
# Three discoveries, none of them a list in this file: the public Go packages
# come from the ledger docs/public_api.md, the runnable programs from
# `git ls-files` filtered to `package main`, and the format values from the
# declarations the code decides with. The program discovery is why `cmd` appears
# at all -- `cmd/main.go` is three lines and a call to root.Execute, a fourth
# complete copy of the native CLI that no release, no gate and no earlier
# inventory has ever mentioned.
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
	run_feature_inventory_gate "check-inventory-surfaces" 'TestInventorySurfaces_' \
		"every public Go package, installable program and enumerated format value is claimed by a row"
	;;
--selftest)
	run_feature_inventory_gate "check-inventory-surfaces --selftest" 'TestInventorySurfacesSelftest_' \
		"the fixtures each plant an unclaimed surface, a claim that names nothing, or a discovery set that came back empty"
	;;
*)
	feature_inventory_usage "$0"
	;;
esac

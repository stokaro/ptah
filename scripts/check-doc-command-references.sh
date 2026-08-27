#!/usr/bin/env bash
# Checks tracked documentation for invocations of commands that no longer exist.
#
# Three bounded readings, never prose: a fenced block whose info string names a
# shell, a heading whose text is a backticked command path, and a table row
# inside a section such a heading opens. Prose is where a document says a command
# does NOT exist -- AGENTS.md says exactly that about `ptah generate` -- and a
# scan of the whole text flags 105 sentences, almost every one of them correct.
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
	run_feature_inventory_gate "check-doc-command-references" 'TestDocCommandReferences_' \
		"no tracked document invokes a command the trees do not answer"
	;;
--selftest)
	run_feature_inventory_gate "check-doc-command-references --selftest" 'TestDocCommandReferencesSelftest_' \
		"the fixtures each plant a stale invocation, or one of the shapes the scan must not read"
	;;
*)
	feature_inventory_usage "$0"
	;;
esac

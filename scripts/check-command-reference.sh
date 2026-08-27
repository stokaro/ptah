#!/usr/bin/env bash
# Checks the generated command reference against the trees it renders.
#
# `--write` regenerates the block instead of checking it.
#
# The marker and emptiness rules are not padding. A file whose markers were
# renamed or lost in a merge would yield an empty block on both sides, the
# comparison would find them identical, and a gate that compares nothing to
# nothing reports success at exactly the moment it stopped working.
# scripts/check-agent-surface.sh states the same rule in its own header.
#
# `--selftest` runs the fixtures instead: each one plants the defect this gate
# exists to catch, and each one requires the analyzer to name it. A gate nobody
# has watched fail is not a gate.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"
# shellcheck source=scripts/lib/featureinventory.sh
. scripts/lib/featureinventory.sh

# --write is this gate's repair, and it runs the same rendering the check
# compares against: the block is replaced in place, so the prose around it is
# untouched.
if [ "${1-}" = "--write" ]; then
	FEATURE_INVENTORY_COMMAND_REFERENCE_OUT="$repo_root/docs/command-reference.md" \
		go test ./internal/featureinventory -run '^TestCommandReferenceSelftest_TheWriteMode' -count=1 >/dev/null
	echo "check-command-reference: rewrote the generated block in docs/command-reference.md"
	exit 0
fi

case "${1-}" in
"")
	run_feature_inventory_gate "check-command-reference" 'TestCommandReference_' \
		"the generated block in docs/command-reference.md matches the command trees"
	;;
--selftest)
	run_feature_inventory_gate "check-command-reference --selftest" 'TestCommandReferenceSelftest_' \
		"the fixtures each plant a stale row, a missing marker, or a block with nothing between the markers"
	;;
*)
	feature_inventory_usage "$0 [--write]"
	;;
esac

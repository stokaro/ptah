#!/usr/bin/env bash
# Proves check-action-run.sh notices when the action's run script stops passing
# an input to a command that reads it.
#
# Each mutation removes one hand-off from a COPY of .github/actions/ptah/run.sh
# and requires the check to go red. PTAH_ACTION_RUN_SCRIPT aims the check at the
# copy, so the tracked script is never edited. The mutations are python3
# programs in single quotes, so every $ in them belongs to python or to the
# script's own text.
# shellcheck disable=SC2016

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

script="$repo_root/.github/actions/ptah/run.sh"
gate="$repo_root/scripts/check-action-run.sh"

work="$(mktemp -d "${TMPDIR:-/tmp}/ptah-action-run-selftest.XXXXXX")"
trap 'rm -rf "$work"' EXIT INT TERM

failures=0

# run_mutation replaces one exact piece of the script's text, requires the text
# to exist exactly once, and requires the check to refuse the result.
run_mutation() {
	local description="$1" old="$2" new="$3"
	local copy="$work/run.sh"

	cp "$script" "$copy"
	MUTANT="$copy" OLD="$old" NEW="$new" python3 -c '
import os
path = os.environ["MUTANT"]
text = open(path).read()
old = os.environ["OLD"]
if text.count(old) != 1:
    raise SystemExit("anchor found %d times: %r" % (text.count(old), old))
open(path, "w").write(text.replace(old, os.environ["NEW"]))
' || {
		echo "check-action-run-selftest: the anchor for \"$description\" has moved;" >&2
		echo "  the mutation would measure the unmodified script" >&2
		failures=$((failures + 1))
		return
	}

	local status=0
	PTAH_ACTION_RUN_SCRIPT="$copy" bash "$gate" >"$work/gate.log" 2>&1 || status=$?
	if [ "$status" -eq 0 ]; then
		echo "check-action-run-selftest: the check PASSED with $description" >&2
		sed 's/^/    | /' "$work/gate.log" >&2
		failures=$((failures + 1))
		return
	fi
	printf '  %s\n' "$description"
}

echo "check-action-run-selftest: the check must accept the script as it stands"
if ! bash "$gate" >"$work/control.log" 2>&1; then
	echo "check-action-run-selftest: the check fails on the UNMODIFIED script;" >&2
	echo "  the mutations below would prove nothing" >&2
	sed 's/^/    | /' "$work/control.log" >&2
	exit 1
fi

echo
echo "check-action-run-selftest: removing one hand-off at a time and requiring the check to go red"
run_mutation "allow-value-rounding no longer reaches the plans" \
	'	common_args+=(--allow-value-rounding)' \
	'	:'
run_mutation "allow-value-rounding no longer reaches generate" \
	'		generate_args+=(--allow-value-rounding)' \
	'		:'
run_mutation "allow-value-rounding is passed whatever the input says" \
	'if [[ "${INPUT_ALLOW_VALUE_ROUNDING:-false}" == "true" ]]; then' \
	'if true; then'
run_mutation "the generate step no longer runs" \
	'if [[ "${INPUT_GENERATE:-false}" == "true" ]]; then
	migration_dir=' \
	'if false; then
	migration_dir='

if [ "$failures" -ne 0 ]; then
	echo "check-action-run-selftest: $failures mutation(s) were not refused" >&2
	exit 1
fi
echo
echo "check-action-run-selftest: OK"

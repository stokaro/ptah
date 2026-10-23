#!/usr/bin/env bash
# Proves check-action-install.sh notices when the action installer stops
# verifying the archive it downloads.
#
# Each mutation removes one refusal from a COPY of .github/actions/ptah/install.sh
# and requires the gate to go red. PTAH_ACTION_INSTALL_SCRIPT aims the gate at
# the copy, so the tracked installer is never edited. The mutations are python3
# programs in single quotes, so every $ in them belongs to python or to the
# installer's own text.
# shellcheck disable=SC2016

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

installer="$repo_root/.github/actions/ptah/install.sh"
gate="$repo_root/scripts/check-action-install.sh"

work="$(mktemp -d "${TMPDIR:-/tmp}/ptah-action-selftest.XXXXXX")"
trap 'rm -rf "$work"' EXIT INT TERM

failures=0

# run_mutation replaces one exact piece of the installer's text, requires the
# text to exist exactly once, and requires the gate to refuse the result.
run_mutation() {
	local description="$1" old="$2" new="$3"
	local copy="$work/install.sh"

	cp "$installer" "$copy"
	MUTANT="$copy" OLD="$old" NEW="$new" python3 -c '
import os
path = os.environ["MUTANT"]
text = open(path).read()
old = os.environ["OLD"]
if text.count(old) != 1:
    raise SystemExit("anchor found %d times: %r" % (text.count(old), old))
open(path, "w").write(text.replace(old, os.environ["NEW"]))
' || {
		echo "check-action-install-selftest: the anchor for \"$description\" has moved;" >&2
		echo "  the mutation would measure the unmodified installer" >&2
		failures=$((failures + 1))
		return
	}

	local status=0
	PTAH_ACTION_INSTALL_SCRIPT="$copy" bash "$gate" >"$work/gate.log" 2>&1 || status=$?
	if [ "$status" -eq 0 ]; then
		echo "check-action-install-selftest: the gate PASSED with $description" >&2
		sed 's/^/    | /' "$work/gate.log" >&2
		failures=$((failures + 1))
		return
	fi
	printf '  %s\n' "$description"
}

echo "check-action-install-selftest: the gate must accept the installer as it stands"
if ! bash "$gate" >"$work/control.log" 2>&1; then
	echo "check-action-install-selftest: the gate fails on the UNMODIFIED installer;" >&2
	echo "  the mutations below would prove nothing" >&2
	sed 's/^/    | /' "$work/control.log" >&2
	exit 1
fi

echo
echo "check-action-install-selftest: removing one refusal at a time and requiring the gate to go red"
run_mutation "a wrong digest is accepted" \
	'if [[ "$want" != "$got" ]]; then' \
	'if false; then'
run_mutation "an archive with no checksums.txt entry is accepted" \
	'if [[ -z "$want" ]]; then' \
	'if false; then'
run_mutation "a release with no checksums.txt falls through to the source build" \
	'	*)
		printf '"'"'refusing to install: the release has an archive for %s/%s but no checksums.txt to verify it\n'"'"' "$os" "$arch" >&2
		exit 1
		;;' \
	'	*)
		install_from_source
		exit 0
		;;'

if [ "$failures" -ne 0 ]; then
	echo "check-action-install-selftest: $failures mutation(s) were not refused" >&2
	exit 1
fi
echo "check-action-install-selftest: every mutation was refused"

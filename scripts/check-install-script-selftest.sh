#!/usr/bin/env bash
# Proves check-install-script.sh notices when the installer stops doing what it
# claims.
#
# The suite passed on its first run, which is the state that says nothing: a
# test that has only ever been observed green is indistinguishable from one that
# asserts nothing. Each mutation below reintroduces a defect that a real
# installer in the wild has -- verification that turns itself off, an
# architecture guessed at instead of refused, curl without -f, a shell startup
# file edited on the user's behalf -- and requires the gate to go red.
#
# Every mutation is applied to a COPY. PTAH_INSTALL_SCRIPT is how the gate is
# aimed at it, so the tracked installer is never edited and a failed run leaves
# nothing behind.
#
# The mutated runs use one shell rather than the whole matrix. The subject here
# is which assertions the gate makes, not which shells it makes them under, and
# nine full matrices would cost minutes to say the same thing.
#
# The mutations below are python3 programs written as single-quoted shell
# arguments, so every $ in them belongs to python or to the installer's own
# text. None of them is a shell expansion that failed to happen.
# shellcheck disable=SC2016

set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

installer="$repo_root/docs/site/public/install.sh"
gate="$repo_root/scripts/check-install-script.sh"

work="$(mktemp -d "${TMPDIR:-/tmp}/ptah-install-selftest.XXXXXX")"
trap 'rm -rf "$work"' EXIT INT TERM

failures=0
checked=0

# run_mutation applies one edit to a copy of the installer and requires the gate
# to refuse it. The edit is a python3 expression over the file's text, because
# every one of these is a substring replacement and sed's escaping would bury
# what is being changed.
run_mutation() {
	local description="$1" edit="$2"
	local copy="$work/install.sh"
	checked=$((checked + 1))

	cp "$installer" "$copy"
	MUTANT="$copy" python3 -c "$edit"

	if cmp -s "$installer" "$copy"; then
		echo "check-install-script-selftest: the mutation \"$description\" changed nothing;" >&2
		echo "  its anchor has moved and it is measuring the unmodified installer" >&2
		failures=$((failures + 1))
		return
	fi

	local status=0
	PTAH_INSTALL_SCRIPT="$copy" PTAH_INSTALL_TEST_SHELLS=/bin/sh \
		bash "$gate" >"$work/gate.log" 2>&1 || status=$?

	if [ "$status" -eq 0 ]; then
		echo "check-install-script-selftest: the gate PASSED with $description" >&2
		sed 's/^/    | /' "$work/gate.log" >&2
		failures=$((failures + 1))
		return
	fi
	printf '  %s\n' "$description"
}

echo "check-install-script-selftest: the gate must accept the installer as it stands"
if ! PTAH_INSTALL_TEST_SHELLS=/bin/sh bash "$gate" >"$work/control.log" 2>&1; then
	echo "check-install-script-selftest: the gate fails on the UNMODIFIED installer;" >&2
	echo "  the mutations below would prove nothing" >&2
	sed 's/^/    | /' "$work/control.log" >&2
	exit 1
fi

echo
echo "check-install-script-selftest: breaking one property at a time and requiring a refusal"

# The comparison itself. Everything else in the verification path can be intact
# and this one line decides whether a tampered archive is installed.
run_mutation "the checksum comparison neutered" '
import os, pathlib
p = pathlib.Path(os.environ["MUTANT"])
s = p.read_text()
old = "\tif [ \"$want\" != \"$got\" ]; then"
assert old in s, "anchor moved: the checksum comparison"
p.write_text(s.replace(old, "\tif false; then"))
'

# The line that closes the vacuous pass. Without it an asset checksums.txt never
# mentions verifies against nothing at all, which is what macOS sha256sum -c
# does with an empty file: silence, exit 0.
run_mutation "the refusal of an absent checksum line removed" '
import os, pathlib
p = pathlib.Path(os.environ["MUTANT"])
s = p.read_text()
old = "\tif [ -z \"$want\" ]; then\n\t\tfail 6 \"checksums.txt has no entry for $name\"\n\tfi"
assert old in s, "anchor moved: the empty-checksum refusal"
p.write_text(s.replace(old, "\tif false; then\n\t\tfail 6 \"checksums.txt has no entry for $name\"\n\tfi"))
'

# uv, verbatim: verification that turns itself off when its tool is missing and
# prints a line about it. The run then reports the same success as one that
# verified, which is why this cannot be a warning.
run_mutation "verification skipped when no sha256 tool is present" '
import os, pathlib
p = pathlib.Path(os.environ["MUTANT"])
s = p.read_text()
old = "\t\tfail 4 \"need one of sha256sum, shasum or openssl to verify the download; refusing to install an unverified archive\""
assert old in s, "anchor moved: the hasher refusal"
s = s.replace(old, "\t\thasher=none")
old2 = "\tlocal file=\"$1\" name=\"$2\" checksums=\"$3\" pattern want got"
assert old2 in s, "anchor moved: verify_checksum"
s = s.replace(old2, old2 + "\n\tif [ \"$hasher\" = none ]; then\n\t\tsay \"skipping sha256 checksum verification\"\n\t\treturn 0\n\tfi")
p.write_text(s)
'

# deno, verbatim: the default arm of the architecture case. An armv7 machine
# downloads an amd64 build and finds out when it runs.
run_mutation "an unknown architecture defaulting to amd64" '
import os, pathlib
p = pathlib.Path(os.environ["MUTANT"])
s = p.read_text()
old = "\taarch64 | arm64) arch=arm64 ;;\n\t*) arch=\"\" ;;"
assert old in s, "anchor moved: the architecture case"
s = s.replace(old, "\taarch64 | arm64) arch=arm64 ;;\n\t*) arch=amd64 ;;")
old2 = "\tDarwin) os=darwin ;;\n\t*) os=\"\" ;;"
assert old2 in s, "anchor moved: the operating-system case"
s = s.replace(old2, "\tDarwin) os=darwin ;;\n\t*) os=linux ;;")
p.write_text(s)
'

# Our own documented instructions, before this script existed: curl without -f
# exits 0 on a 404 and writes a file containing "Not Found". The install then
# fails as an integrity error for a file that was never a download.
run_mutation "curl invoked without -f" '
import os, pathlib
p = pathlib.Path(os.environ["MUTANT"])
s = p.read_text()
assert "\t\tprintf -- \x27-fsSL\x27" in s, "anchor moved: the curl flags"
s = s.replace("\t\tprintf -- \x27-fsSL\x27", "\t\tprintf -- \x27-sSL\x27")
s = s.replace("\t\tprintf -- \x27-fL --progress-bar\x27", "\t\tprintf -- \x27-L --progress-bar\x27")
p.write_text(s)
'

# A destructive statement at the top level. The whole reason every step lives in
# a function and the only top-level call is the last line is that a pipe cut
# mid-transfer must not half-install; a statement here runs from a fragment.
run_mutation "a destructive statement outside every function" '
import os, pathlib
p = pathlib.Path(os.environ["MUTANT"])
s = p.read_text()
old = "set -eu\n"
assert old in s, "anchor moved: set -eu"
p.write_text(s.replace(old, old + "\nmkdir -p \"${PTAH_INSTALL_DIR:-/tmp}\" && : >\"${PTAH_INSTALL_DIR:-/tmp}/ptah\"\n", 1))
'

# uv, bun and nvm all do this. It is the loudest complaint against installers
# read from a pipe, and it is invisible until someone reads their own .bashrc.
run_mutation "a shell startup file edited on the reader s behalf" '
import os, pathlib
p = pathlib.Path(os.environ["MUTANT"])
s = p.read_text()
old = "\tprintf \x27ptah: add it for this shell:\\n\x27"
assert old in s, "anchor moved: the PATH advice"
p.write_text(s.replace(old, "\tprintf \x27export PATH=\"%s:$PATH\"\\n\x27 \"$bindir\" >>\"$HOME/.bashrc\"\n" + old))
'

# A boolean that lands on a default when it cannot be read. PTAH_INSTALL_QUIET=ture
# in a CI environment file is then a silent no-op rather than the error it is.
run_mutation "a malformed boolean falling back to the default" '
import os, pathlib
p = pathlib.Path(os.environ["MUTANT"])
s = p.read_text()
old = "\t*) fail 2 \"invalid boolean value \\\"$value\\\" for $name; use 1, 0, true, false, yes or no\" ;;"
assert old in s, "anchor moved: parse_bool"
p.write_text(s.replace(old, "\t*) BOOL_RESULT=false ;;"))
'

# An install directory nobody proved writable, checked after the transfer rather
# than before it. The reader waits for tens of megabytes to learn what was
# knowable at the start.
run_mutation "the install directory not proven writable" '
import os, pathlib
p = pathlib.Path(os.environ["MUTANT"])
s = p.read_text()
old = "\tif ! (: >\"$probe\") 2>/dev/null; then"
assert old in s, "anchor moved: the writability probe"
p.write_text(s.replace(old, "\tif false; then"))
'

# The shape --verify-signature had for one revision: the cosign check sat inside
# verify_signature, so a machine without cosign was told so only after the
# archive had been downloaded for it. The exit code is the tell -- 6 for a
# verification that could not run, where a missing prerequisite is 4.
run_mutation "the cosign check moved back to verification time" '
import os, pathlib
p = pathlib.Path(os.environ["MUTANT"])
s = p.read_text()
old = "\tdetect_hasher\n\tdetect_signature_tool\n"
assert old in s, "anchor moved: the prerequisite block"
p.write_text(s.replace(old, "\tdetect_hasher\n"))
'

echo
if [ "$failures" -ne 0 ]; then
	echo "check-install-script-selftest: $failures of $checked mutations went unnoticed" >&2
	exit 1
fi
echo "check-install-script-selftest: OK ($checked mutations, each one refused)"

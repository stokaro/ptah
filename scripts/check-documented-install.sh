#!/usr/bin/env bash
# Runs every `go install` command the documentation publishes, and fails if any
# of them does not produce a working binary.
#
# The commands are discovered from the tracked files rather than listed here, so
# a newly documented install command is covered the day it is written instead of
# the day someone remembers to add it.
#
# Discovery failing closed is the point of MIN_EXPECTED. A grep that silently
# matches nothing would make this script exit 0 while testing nothing at all,
# which is the failure mode it exists to prevent -- the documented commands were
# broken for days precisely because no job ran them.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

readonly MIN_EXPECTED=2

# A read loop rather than mapfile: mapfile is bash 4+, and the bash shipped on
# macOS is 3.2, so a developer running this locally would otherwise get
# "command not found" and, because it sits in a pipeline, a zero exit status.
commands=()
while IFS= read -r line; do
	commands+=("$line")
done < <(
	git grep -hoE 'go install go\.5x5\.cz/ptah/cmd/[a-z-]+@latest' -- \
		'*.md' '*.mdx' '*.yml' '*.yaml' '*.sh' |
		sort -u
)

if [ "${#commands[@]}" -lt "$MIN_EXPECTED" ]; then
	echo "install-smoke: found ${#commands[@]} documented install command(s), expected at least $MIN_EXPECTED" >&2
	echo "install-smoke: either the docs changed shape or the pattern stopped matching; refusing to pass without testing anything" >&2
	exit 1
fi

bindir="$(mktemp -d)"
trap 'rm -rf "$bindir"' EXIT

echo "install-smoke: ${#commands[@]} documented command(s)"
failed=0

for command in "${commands[@]}"; do
	binary="${command#go install go.5x5.cz/ptah/cmd/}"
	binary="${binary%@latest}"

	echo "-- $command"
	if ! GOBIN="$bindir" GOFLAGS= $command; then
		echo "install-smoke: FAILED to install $binary" >&2
		failed=1
		continue
	fi

	# Installing is not enough: a vanity-host or module problem can still leave a
	# binary that cannot run, so the installed program has to say something.
	#
	# No single spelling worked for all of them, measured on v0.2.0:
	#
	#   ptah          version -> "Version: ..."   --version -> "ptah version ..."
	#   ptah-compat   version -> "Version: ..."   --version -> unknown flag
	#   ptah-ls       version -> empty, exit 0    --version -> "Version: ..."
	#
	# stokaro/ptah#1064 fixed that: `version` now answers on all three binaries,
	# and on ptah every spelling prints identical bytes. This script keeps trying
	# both anyway, because it installs the PUBLISHED module rather than the
	# checkout and will keep meeting pre-fix releases for as long as they are the
	# latest tag. For the same reason it is not evidence that the fix landed --
	# cmd/ptah-ls/main_test.go is what gates that.
	#
	# The answer must be NON-EMPTY. Exit status alone would pass the pre-fix
	# ptah-ls without it having said anything, which is the exact shape of
	# failure this script exists to catch.
	#
	# stdin is closed because one of these binaries is a language server, and a
	# check that can block forever is not a check. The `|| true` keeps a failing
	# spelling from aborting the script under `set -e` before it can report.
	reported="$("$bindir/$binary" version </dev/null 2>/dev/null | head -1 || true)"
	if [ -z "$reported" ]; then
		reported="$("$bindir/$binary" --version </dev/null 2>/dev/null | head -1 || true)"
	fi
	if [ -z "$reported" ]; then
		echo "install-smoke: $binary installed but neither 'version' nor '--version' printed anything" >&2
		failed=1
		continue
	fi
	echo "   ok: $reported"
done

if [ "$failed" -ne 0 ]; then
	echo "install-smoke: at least one documented install command is broken" >&2
	exit 1
fi

echo "install-smoke: every documented install command works"

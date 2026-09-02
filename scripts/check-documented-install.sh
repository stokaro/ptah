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

# What the product says it releases. `.goreleaser.yaml` is the declaration --
# `go list` can find main packages, it cannot know which ones ship -- and every
# released binary is one a reader can be told to `go install`, so this is also
# the number of install commands the documentation is expected to publish.
#
# Read as text because this script has no YAML parser and adding one to buy a
# three-line list would be the larger change. The guard is below: an empty read
# fails rather than turning the loop into a no-op.
#
# A hand-written count sat here instead, and it was a number that was true when
# it was written (stokaro/ptah#2799).
released=()
while IFS= read -r line; do
	released+=("$line")
done < <(
	grep -oE '^[[:space:]]*binary:[[:space:]]*[A-Za-z0-9_-]+' .goreleaser.yaml |
		awk '{print $2}' |
		sort -u
)

if [ "${#released[@]}" -eq 0 ]; then
	echo "install-smoke: .goreleaser.yaml declares no binary:, so there is nothing to expect" >&2
	echo "install-smoke: refusing to pass without testing anything" >&2
	exit 1
fi

# The PACKAGE the documentation names, not the whole command. A page telling a
# reader to pin a version writes `@vX.Y.Z`, which is the right thing for that
# page and is not installable as written; the question this script asks -- does
# the package install and does the binary run -- does not depend on how the
# version is spelled. Requiring `@latest` here made two of the three released
# binaries stop being covered the day the placeholder landed, and the floor
# below is what reported it.
#
# A read loop rather than mapfile: mapfile is bash 4+, and the bash shipped on
# macOS is 3.2, so a developer running this locally would otherwise get
# "command not found" and, because it sits in a pipeline, a zero exit status.
documented=()
while IFS= read -r line; do
	documented+=("$line")
done < <(
	git grep -hoE 'go install go\.5x5\.cz/ptah/cmd/[A-Za-z0-9_-]+@' -- \
		'*.md' '*.mdx' '*.yml' '*.yaml' '*.sh' |
		sed -e 's|^go install go\.5x5\.cz/ptah/cmd/||' -e 's|@$||' |
		sort -u
)

# Nothing found at all is reported here rather than left to `set -u`. Under the
# bash 3.2 macOS ships, expanding an empty array with "${a[@]}" is an unbound
# variable and the script dies with a shell diagnostic instead of the sentence
# below -- and a newer bash expands it to nothing, so the difference is
# invisible on a CI runner and only bites the person running this locally.
if [ "${#documented[@]}" -eq 0 ]; then
	echo "install-smoke: found no documented 'go install' command at all" >&2
	echo "install-smoke: either the docs changed shape or the pattern stopped matching; refusing to pass without testing anything" >&2
	exit 1
fi

# Every released binary has to be among them. This is the assertion the floor
# was standing in for, and it is derived rather than counted: a fourth released
# binary is covered the day it ships, and one that stops being documented fails
# here instead of quietly shrinking the run.
missing=()
for binary in "${released[@]}"; do
	found=0
	for candidate in "${documented[@]}"; do
		if [ "$candidate" = "$binary" ]; then
			found=1
		fi
	done
	if [ "$found" -eq 0 ]; then
		missing+=("$binary")
	fi
done

if [ "${#missing[@]}" -ne 0 ]; then
	echo "install-smoke: no documented 'go install' command names ${missing[*]}" >&2
	echo "install-smoke: .goreleaser.yaml releases it, so a reader needs a command for it" >&2
	exit 1
fi

# Installed at @latest whatever the page spelled: that is the published module,
# which is what this script has always installed and what a reader following a
# pinned page would get with their own tag substituted.
commands=()
for binary in "${documented[@]}"; do
	commands+=("go install go.5x5.cz/ptah/cmd/$binary@latest")
done

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

#!/usr/bin/env bash

set -euo pipefail

# Keep developer-specific Unix paths and conventional local checkout defaults
# out of tracked files. The leading-character constraint avoids matching
# synthetic Windows paths such as C:/Users/runner in cross-platform tests.
pattern="(^|[[:space:]=\"(\`])/(Users|home)/[^/[:space:]]+|\\\$HOME/(Work|Projects)/"

# What the scan covers, counted separately. `git grep` reports 1 both for
# "scanned everything and matched nothing" and for "scanned nothing", so on its
# own this gate cannot tell a clean tree from a scan that stopped covering the
# repository -- a pathspec that matched no file passes it silently. Every other
# gate here that discovers its own input already refuses a vacuous pass; this
# was the one that did not.
scanned=0
while IFS= read -r _; do
	((++scanned))
done < <(git ls-files -- . ':(exclude)scripts/check-repository-local-paths.sh')

if ((scanned == 0)); then
	printf 'repository-local path check scanned no tracked files; refusing to report a vacuous pass\n' >&2
	exit 1
fi

status=0
matches="$(git grep -nE "$pattern" -- . ':(exclude)scripts/check-repository-local-paths.sh')" || status=$?

if ((status > 1)); then
	printf 'repository-local path check failed with exit code %d\n' "$status" >&2
	exit "$status"
fi
if ((status == 0)); then
	printf 'repository-local paths found in tracked files:\n%s\n' "$matches" >&2
	exit 1
fi

printf 'repository-local paths: OK (%d tracked files scanned)\n' "$scanned"

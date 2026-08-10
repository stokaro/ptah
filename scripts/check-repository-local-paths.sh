#!/usr/bin/env bash

set -euo pipefail

# Keep developer-specific Unix paths and conventional local checkout defaults
# out of tracked files. The leading-character constraint avoids matching
# synthetic Windows paths such as C:/Users/runner in cross-platform tests.
pattern="(^|[[:space:]=\"(\`])/(Users|home)/[^/[:space:]]+|\\\$HOME/(Work|Projects)/"

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

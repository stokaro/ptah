#!/usr/bin/env bash
# Break one canonical representation and require the equivalence gate to fail.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-source-equivalence-selftest.XXXXXX")"
cleanup() {
	rm -rf "$work_dir"
}
trap cleanup EXIT

fixture="$work_dir/fixture"
cp -R "$repo_root/docs/site/fixtures/source-equivalence" "$fixture"

ptah_bin="${PTAH_BIN:-$work_dir/ptah}"
if [ -z "${PTAH_BIN:-}" ]; then
	(
		cd "$repo_root"
		go build -o "$ptah_bin" ./cmd/ptah
	)
fi

PTAH_BIN="$ptah_bin" PTAH_SOURCE_FIXTURE="$fixture" \
	"$repo_root/scripts/check-source-equivalence.sh" >/dev/null 2>&1

perl -0pi -e 's/^      title:/      headline:/m' "$fixture/schema.yaml"
status=0
PTAH_BIN="$ptah_bin" PTAH_SOURCE_FIXTURE="$fixture" \
	"$repo_root/scripts/check-source-equivalence.sh" >/dev/null 2>&1 || status=$?

if [ "$status" -eq 0 ]; then
	echo "check-source-equivalence-selftest: the gate accepted a divergent YAML schema" >&2
	exit 1
fi

echo "check-source-equivalence-selftest: the gate rejects a divergent source fixture"

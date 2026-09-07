#!/usr/bin/env bash
# Proves check-app-token-lifetime.sh fails on the shapes it exists to catch.
#
# The guard passes on the tree as it stands, and a guard whose only observed
# result is "pass" is indistinguishable from one that examines nothing. Each
# case below builds one workflow in a throwaway checkout and requires the
# verdict the guard's header promises.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-app-token-selftest.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

# A git repository, because the guard resolves its own root with rev-parse.
mkdir -p "$work_dir/.github/workflows" "$work_dir/scripts"
git -C "$work_dir" init --quiet
cp "$repo_root/scripts/check-app-token-lifetime.sh" "$work_dir/scripts/"

write_workflow() {
	cat >"$work_dir/.github/workflows/example.yml"
}

guard_accepts() {
	(cd "$work_dir" && bash scripts/check-app-token-lifetime.sh >/dev/null 2>&1)
}

# The defect: a minting job that may run past the token's hour.
write_workflow <<'YAML'
name: Example

on:
  push:
    branches: [master]

jobs:
  publish:
    runs-on: ubuntu-latest
    timeout-minutes: 90
    steps:
      - uses: actions/create-github-app-token@v3
        with:
          app-id: ${{ vars.PUBLISH_APP_ID }}
          private-key: ${{ secrets.PUBLISH_APP_KEY }}
      - run: echo publish
YAML

if guard_accepts; then
	echo "check-app-token-lifetime-selftest: the guard ACCEPTED a minting job capped at 90 minutes" >&2
	exit 1
fi

# The same defect written as an absent cap, which is the unbounded case rather
# than a large one. Without this row the guard could read only the number and
# pass every job that declares none.
write_workflow <<'YAML'
name: Example

on:
  push:
    branches: [master]

jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/create-github-app-token@v3
        with:
          app-id: ${{ vars.PUBLISH_APP_ID }}
          private-key: ${{ secrets.PUBLISH_APP_KEY }}
      - run: echo publish
YAML

if guard_accepts; then
	echo "check-app-token-lifetime-selftest: the guard ACCEPTED a minting job with no timeout-minutes" >&2
	exit 1
fi

# The repair: a cap below the token's life. The same file must now be accepted,
# which is what separates "refuses this shape" from "refuses everything".
write_workflow <<'YAML'
name: Example

on:
  push:
    branches: [master]

jobs:
  publish:
    runs-on: ubuntu-latest
    timeout-minutes: 40
    steps:
      - uses: actions/create-github-app-token@v3
        with:
          app-id: ${{ vars.PUBLISH_APP_ID }}
          private-key: ${{ secrets.PUBLISH_APP_KEY }}
      - run: echo publish
YAML

if ! guard_accepts; then
	echo "check-app-token-lifetime-selftest: the guard REFUSED a minting job capped at 40 minutes" >&2
	exit 1
fi

# A job that mints nothing may run as long as it likes, and the rule is about
# the token rather than about long jobs. The compliant minting job beside it
# keeps the sweep non-empty, so this row measures the long job alone.
write_workflow <<'YAML'
name: Example

on:
  push:
    branches: [master]

jobs:
  publish:
    runs-on: ubuntu-latest
    timeout-minutes: 40
    steps:
      - uses: actions/create-github-app-token@v3
        with:
          app-id: ${{ vars.PUBLISH_APP_ID }}
          private-key: ${{ secrets.PUBLISH_APP_KEY }}
      - run: echo publish
  build:
    runs-on: ubuntu-latest
    timeout-minutes: 180
    steps:
      - run: echo a long build that mints nothing
YAML

if ! guard_accepts; then
	echo "check-app-token-lifetime-selftest: the guard REFUSED a long job that mints no token" >&2
	exit 1
fi

# The empty-sweep guard: a workflow directory where nothing mints must fail
# rather than report a clean sweep of nothing. The awk extraction is the part
# that can break silently, and a broken one finds no minting job at all.
write_workflow <<'YAML'
name: Example

on:
  push:
    branches: [master]

jobs:
  build:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - run: echo hi
YAML

if guard_accepts; then
	echo "check-app-token-lifetime-selftest: the guard reported success having examined no minting job" >&2
	exit 1
fi

echo "check-app-token-lifetime-selftest: the guard refuses an uncapped and an over-capped minting job, and accepts the repair"

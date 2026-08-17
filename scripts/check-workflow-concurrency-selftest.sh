#!/usr/bin/env bash
# Proves check-workflow-concurrency.sh fails on the shape it exists to catch.
#
# The guard passes on the tree as it stands, and a guard whose only observed
# result is "pass" is indistinguishable from one that examines nothing. This
# reconstructs the defect in a throwaway checkout and requires a refusal, then
# repairs it and requires acceptance.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-concurrency-selftest.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

# A git repository, because the guard resolves its own root with rev-parse.
mkdir -p "$work_dir/.github/workflows" "$work_dir/scripts"
git -C "$work_dir" init --quiet
cp "$repo_root/scripts/check-workflow-concurrency.sh" "$work_dir/scripts/"

# The defect: a pull-request workflow whose group carries no ref.
cat >"$work_dir/.github/workflows/example.yml" <<'YAML'
name: Example

on:
  push:
    branches: [master]
  pull_request:

concurrency:
  group: example-shared
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
YAML

if (cd "$work_dir" && bash scripts/check-workflow-concurrency.sh >/dev/null 2>&1); then
	echo "check-workflow-concurrency-selftest: the guard ACCEPTED a refless group on a pull-request workflow" >&2
	exit 1
fi

# The repair: a ref in the group. The same file must now be accepted, which is
# what separates "refuses this shape" from "refuses everything".
cat >"$work_dir/.github/workflows/example.yml" <<'YAML'
name: Example

on:
  push:
    branches: [master]
  pull_request:

concurrency:
  group: example-${{ github.event_name == 'pull_request' && github.ref || 'deploy' }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
YAML

if ! (cd "$work_dir" && bash scripts/check-workflow-concurrency.sh >/dev/null 2>&1); then
	echo "check-workflow-concurrency-selftest: the guard REFUSED a per-ref group" >&2
	exit 1
fi

# A workflow with no pull_request trigger may share one slot: a nightly or a
# deploy has nobody to starve. Without this row the guard could be tightened
# into refusing every shared group and the self-test would not notice.
cat >"$work_dir/.github/workflows/example.yml" <<'YAML'
name: Nightly

on:
  schedule:
    - cron: '40 2 * * *'
  workflow_dispatch:

concurrency:
  group: nightly-shared
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
YAML

if ! (cd "$work_dir" && bash scripts/check-workflow-concurrency.sh >/dev/null 2>&1); then
	echo "check-workflow-concurrency-selftest: the guard REFUSED a shared group on a workflow with no pull_request trigger" >&2
	exit 1
fi

# The empty-sweep guard: a workflow directory with no concurrency block at all
# must fail rather than report a clean sweep of nothing.
cat >"$work_dir/.github/workflows/example.yml" <<'YAML'
name: Plain

on:
  pull_request:

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo hi
YAML

if (cd "$work_dir" && bash scripts/check-workflow-concurrency.sh >/dev/null 2>&1); then
	echo "check-workflow-concurrency-selftest: the guard reported success having examined no group" >&2
	exit 1
fi

echo "check-workflow-concurrency-selftest: the guard refuses a refless pull-request group and accepts the repair"

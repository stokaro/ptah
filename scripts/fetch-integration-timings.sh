#!/usr/bin/env bash
# fetch-integration-timings.sh OUTPUT
#
# Writes to OUTPUT the contour timings the newest push run of
# go-integration-tests.yml on master measured, for testcontour to balance its
# shards with. The file is the `integration-timings` artifact that run's
# `integration-tests` job uploaded.
#
# It never fails the job. The timings decide only which shard a test lands in,
# never whether it runs, and the gate proves the coverage from the reports. So
# when no timings can be fetched, OUTPUT says why and holds no measurement,
# and every shard balances by test count instead.
#
# Every shard of a run reads the one file this script writes, uploaded once by
# the plan job, rather than fetching for itself: a master run finishing between
# two shards' starts would otherwise give them different inputs and different
# partitions.
#
# Needs GH_TOKEN with actions:read, and GITHUB_REPOSITORY.
set -uo pipefail

out="${1:?usage: fetch-integration-timings.sh OUTPUT}"
repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}"
server="${GITHUB_SERVER_URL:-https://github.com}"
workflow=go-integration-tests.yml
artifact=integration-timings
# A master run whose contour was canceled or never reached the gate keeps no
# timings, so the newest few are tried in turn. Older ones are not worth the
# API calls.
runs_to_try=10

mkdir -p "$(dirname "$out")"

measure_none() {
	printf 'contour timings: %s; the shards balance by test count\n' "$1" >&2
	jq -n --arg reason "$1" '{source: ("no timings: " + $reason)}' >"$out"
	exit 0
}

runs=$(gh api "repos/$repo/actions/workflows/$workflow/runs?branch=master&event=push&status=completed&per_page=$runs_to_try" \
	--jq '.workflow_runs[].id') || measure_none "listing the master runs of $workflow failed"

scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT
while read -r run; do
	[[ -n "$run" ]] || continue
	if gh run download "$run" --repo "$repo" --name "$artifact" --dir "$scratch/$run" >/dev/null 2>&1 &&
		jq --arg source "$server/$repo/actions/runs/$run" '.source = $source' \
			"$scratch/$run/timings.json" >"$out"; then
		printf 'contour timings: measured by %s\n' "$server/$repo/actions/runs/$run" >&2
		exit 0
	fi
done <<<"$runs"

measure_none "none of the last $runs_to_try master runs of $workflow kept the $artifact artifact"

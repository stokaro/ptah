#!/usr/bin/env bash
# Report a failed scheduled run of the Atlas CE reference workflow on one issue.
#
# A scheduled run has no pull request to show its result on, and this workflow
# holds the only run of the whole integration contour under the race detector.
# So a red night has to reach someone by itself, and a red week must not arrive
# as seven issues: an open issue takes a comment, and only when none is open
# does a new one start.
#
# The issue is the open one with exactly this title. Listing asks the issues
# themselves, because the search index can lag behind an issue just opened. A
# failed listing stops the script, because reading it as "no issue" would open a
# second one.
#
# Reads RUN_URL, COMMIT and NEEDS, the JSON form of the job's `needs` context. A
# job that ran out of time reports as cancelled, so that counts as failed too.
# gh reads GH_TOKEN and GH_REPO.
# scripts/report-nightly-failure-selftest.sh holds it to the above.
set -euo pipefail

readonly TITLE='Nightly run of "Atlas CE reference" failed'

failed_jobs="$(jq -r '[to_entries[] | select(.value.result == "failure" or .value.result == "cancelled") | .key] | join(", ")' <<<"${NEEDS:?}")"
details="Run: ${RUN_URL:?}
Commit: ${COMMIT:?}
Failed jobs: ${failed_jobs}"

open_issues="$(gh issue list --state open --limit 1000 --json number,title)"
number="$(jq -r --arg title "$TITLE" '[.[] | select(.title == $title) | .number] | min // empty' <<<"$open_issues")"

if [ -n "$number" ]; then
	gh issue comment "$number" --body-file - <<BODY
The scheduled run failed again.

$details
BODY
	echo "commented on issue $number"
else
	gh issue create --title "$TITLE" --label bug --body-file - <<BODY
The scheduled run of the Atlas CE reference workflow failed. That workflow holds the only run of the whole integration contour under the race detector, so a race that only an end-to-end scenario reaches shows up here first.

$details

Each later failure adds a comment here while this issue stays open. Close it when the nightly run is green again, and the next failure opens a new issue.
BODY
	echo "opened an issue"
fi

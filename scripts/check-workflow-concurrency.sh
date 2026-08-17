#!/usr/bin/env bash
# Refuses a workflow that can queue two pull requests into the same concurrency
# slot, because the loser of that race disappears instead of failing.
#
# `concurrency.cancel-in-progress: false` protects a run that has already
# STARTED. It does nothing for one still queued: when a newer run enters the
# group, the queued one is superseded and ends as cancelled with ZERO jobs, and
# a cancelled run with no jobs is absent from the pull request's check rollup
# entirely. So the pull request is green on every check it shows while the gate
# governing its own changes never ran, and nothing on the page distinguishes
# that from the gate passing (stokaro/ptah#1652).
#
# The rule: a workflow that runs on pull requests must put a ref in its
# concurrency group, so each pull request gets its own slot and only its OWN
# earlier run can supersede it. A workflow with no pull_request trigger may share
# one slot -- a nightly or a deploy has nobody to starve.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# refContexts are the expressions that make a group per-ref. github.ref is the
# usual one; head_ref and the event number identify a pull request just as well,
# and a workflow using either is not the defect this refuses.
readonly refContexts='github\.ref|github\.head_ref|github\.event\.number|github\.event\.pull_request\.number'

status=0
checked=0

for workflow in .github/workflows/*.yml; do
	# The group line of the TOP-LEVEL concurrency block. A job-level
	# concurrency block is indented and is a different question: it serializes
	# jobs inside one run, which cannot hide a run from the rollup.
	group="$(awk '/^concurrency:/{inside=1; next} inside && /^[^[:space:]]/{inside=0} inside && /^[[:space:]]+group:/{print; exit}' "$workflow")"
	if [ -z "$group" ]; then
		continue
	fi
	checked=$((checked + 1))

	if ! grep -qE '^[[:space:]]+pull_request:?[[:space:]]*$' "$workflow"; then
		continue
	fi
	if echo "$group" | grep -qE "$refContexts"; then
		continue
	fi

	echo "check-workflow-concurrency: $workflow runs on pull requests and shares one concurrency slot:" >&2
	echo "  $group" >&2
	echo "  A queued run superseded here ends as cancelled with zero jobs, which is ABSENT from the" >&2
	echo "  pull request's checks rather than failing in them. Put a ref in the group, for example:" >&2
	echo "    group: <name>-\${{ github.event_name == 'pull_request' && github.ref || 'deploy' }}" >&2
	status=1
done

# A run that examined nothing must not report success: the extraction above is
# the part that can break silently, and an empty sweep looks identical to a
# clean one.
if [ "$checked" -eq 0 ]; then
	echo "check-workflow-concurrency: no workflow declared a top-level concurrency group; the extraction is broken" >&2
	exit 1
fi

if [ "$status" -eq 0 ]; then
	echo "check-workflow-concurrency: $checked concurrency groups checked, none shares a slot across pull requests"
fi
exit "$status"

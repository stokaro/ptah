#!/usr/bin/env bash
# Decides which capability-matrix cells one run probes, and says why.
#
# The probe fan-out is one job per (dialect, release line) with a container
# each, and it was the larger half of every pull request's checks until the
# queue stopped keeping up (stokaro/ptah#2185). It is opt-in now, which means
# something has to answer the question "why did this run probe nothing?" in a
# place a person reading the pull request will see. That answer is `reason`
# below, and it is printed in every case including the empty one: an absent
# check must not read like a passing one.
#
# Lives in a script rather than inline in the workflow so it can be exercised.
# A workflow's own YAML only runs on GitHub, and `issue_comment` workflows run
# from the default branch, so a pull request that changes this logic could
# otherwise prove nothing about it before merging. See the companion
# select-capability-cells-selftest.sh.
#
# Usage: select-capability-cells.sh <event> <requested> <all-ids-json>
#
# Prints two lines on stdout, in the KEY=VALUE form GITHUB_OUTPUT takes:
#
#	selected=["postgres-18","postgres-17"]
#	reason=requested by hand: postgres
#
# Exits non-zero when a request named something the matrix does not have.
# Answering a typo with silence would be the same defect this whole tier warns
# about, so `postgres-1` is an error rather than a quiet zero.
set -euo pipefail

event="${1:-}"
requested="${2:-}"
all="${3:-}"

if [ -z "$all" ]; then
	echo "select-capability-cells: no cell ids were passed; the matrix must say what exists before a run can select from it" >&2
	exit 2
fi

case "$event" in
schedule)
	request="all"
	reason="the nightly tier 2 run probes every declared cell"
	;;
workflow_dispatch | repository_dispatch)
	request="${requested:-all}"
	reason="requested by hand: ${request}"
	;;
push)
	request="none"
	reason="the probe fan-out does not run on a push; it runs nightly, and on request from the workflow's Run button"
	;;
pull_request | pull_request_target)
	# A pull request probes only what a `/capability-matrix` comment asked for.
	# The caller reads that comment -- it needs the API and this script takes no
	# network -- and passes the result in, so an empty argument here means the
	# pull request asked for nothing.
	request="${requested:-none}"
	reason="requested on this pull request: ${request}"
	;;
*)
	request="none"
	reason="not requested on this event (${event}); comment \`/capability-matrix\` on the pull request to run it, or use the workflow's Run button"
	;;
esac

if [ "$request" = "all" ]; then
	selected="$all"
elif [ "$request" = "none" ] || [ -z "$request" ]; then
	selected="[]"
else
	# A request names cell ids or dialect prefixes. Matching by prefix is what
	# makes `postgres` mean every PostgreSQL line without the requester having to
	# know the line numbers, and the `+ "-"` is what keeps `mysql` from also
	# selecting every MariaDB cell.
	#
	# `. as $w` is load-bearing. Written as `$id | startswith(. + "-")` the pipe
	# rebinds `.` to $id, so every request compared the id to itself and matched
	# nothing -- `postgres` selected zero cells and the run called the request a
	# typo. The selftest pins both halves.
	selected="$(REQUEST="$request" jq -c '
	  [ .[] as $id
	    | ($ENV.REQUEST | split(",") | map(gsub("^\\s+|\\s+$"; "")) | map(select(length > 0))) as $want
	    | select(any($want[]; . as $w | $id == $w or ($id | startswith($w + "-"))))
	    | $id ]' <<<"$all")"
fi

printf 'selected=%s\n' "$selected"
printf 'reason=%s\n' "$reason"

# A request that names something the matrix does not have is a typo, and
# answering it with silence is the same defect this whole tier warns about.
#
# EVERY selector is checked, not the count. `postgres,postgrez` has a valid
# half, so a count-only check passes while the misspelled half is discarded --
# the run then probes fewer cells than were asked for and reports that it
# honored the request.
#
# `all` and `none` are sentinels, not selectors. Both are resolved by the branch
# above and never reach the matching rule, so neither is a name the matrix could
# be expected to have. Validating `all` as though it were a cell id is what
# failed every scheduled run for three nights (stokaro/ptah#2185).
if [ -n "$request" ] && [ "$request" != "none" ] && [ "$request" != "all" ]; then
	unmatched="$(REQUEST="$request" jq -r '
	  . as $ids
	  | ($ENV.REQUEST | split(",") | map(gsub("^\\s+|\\s+$"; "")) | map(select(length > 0)))
	  | map(select(. as $w | ($ids | any(. == $w or startswith($w + "-"))) | not))
	  | join(", ")' <<<"$all")"
	if [ -n "$unmatched" ]; then
		echo "select-capability-cells: requested cells matched nothing: ${unmatched}" >&2
		echo "select-capability-cells: the matrix declares: ${all}" >&2
		exit 1
	fi
fi

#!/usr/bin/env bash
# The GitHub Action published as stokaro/ptah-action has one copy, and it is
# .github/actions/ptah in this repository. This refuses a tree that could not
# be published from it.
#
# It is a gate rather than a step of the publish workflow on purpose: a check
# that only runs when publishing runs reports nothing on the pull request that
# broke it, and the publish is the moment it is most expensive to find out.
set -euo pipefail

action_dir=".github/actions/ptah"
problems=0

fail() {
	printf '%s\n' "check-action-publishable: $1" >&2
	problems=$((problems + 1))
}

# What the published repository needs in order to be a usable action.
for required in action.yml README.md LICENSE; do
	[[ -f "$action_dir/$required" ]] || fail "$action_dir/$required is missing; the published repository needs it"
done

# A file the action reads at runtime has to travel with it. Anything outside
# the directory is present here and absent there, and the failure appears in
# someone else's workflow rather than in ours.
while IFS= read -r escape; do
	fail "$escape refers outside $action_dir, which does not exist once published"
done < <(git grep -nE '\.\./|\$GITHUB_WORKSPACE' -- "$action_dir" || true)

# Every script the action names must exist, so a rename cannot leave the
# published copy calling a file nobody ships.
while IFS= read -r referenced; do
	[[ -f "$action_dir/$referenced" ]] || fail "$action_dir/action.yml runs $referenced, which is not in the directory"
done < <(grep -oE '\$(GITHUB_ACTION_PATH|\{ *github\.action_path *\})/[A-Za-z0-9_.-]+' "$action_dir/action.yml" |
	sed -E 's|.*/||' | sort -u)

# The published README says where it is edited. Without that line the copy
# invites edits that the next publish discards.
grep -q 'published to stokaro/ptah-action from stokaro/ptah' "$action_dir/README.md" ||
	fail "$action_dir/README.md does not say it is published from here"

if [[ "$problems" -ne 0 ]]; then
	printf '%s\n' "check-action-publishable: $problems problem(s)" >&2
	exit 1
fi

printf 'check-action-publishable: OK (%s files, self-contained)\n' "$(git ls-files "$action_dir" | wc -l | tr -d ' ')"

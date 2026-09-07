#!/usr/bin/env bash
# Proves check-action-publishable.sh fails on each shape it exists to catch.
#
# The guard passes on the tree as it stands, and a guard whose only observed
# result is "pass" is indistinguishable from one that examines nothing. Each
# case below reconstructs one defect in a throwaway checkout, requires a
# refusal naming it, then repairs it and requires acceptance.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-action-publishable-selftest.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

action_dir="$work_dir/.github/actions/ptah"
mkdir -p "$action_dir" "$work_dir/scripts"
git -C "$work_dir" init --quiet
cp "$repo_root/scripts/check-action-publishable.sh" "$work_dir/scripts/"

write_valid() {
	cat >"$action_dir/action.yml" <<'YAML'
name: Example
runs:
  using: composite
  steps:
    - shell: bash
      env:
        GITHUB_ACTION_PATH: ${{ github.action_path }}
      run: "$GITHUB_ACTION_PATH/run.sh"
YAML
	printf '#!/usr/bin/env bash\necho ok\n' >"$action_dir/run.sh"
	printf 'This file is published to stokaro/ptah-action from stokaro/ptah.\n' >"$action_dir/README.md"
	printf 'MIT\n' >"$action_dir/LICENSE"
	git -C "$work_dir" add -A
}

expect_refusal() {
	local what="$1" needle="$2" output
	if output="$(cd "$work_dir" && ./scripts/check-action-publishable.sh 2>&1)"; then
		printf 'check-action-publishable-selftest: %s was accepted\n' "$what" >&2
		exit 1
	fi
	if ! printf '%s' "$output" | grep -q "$needle"; then
		printf 'check-action-publishable-selftest: %s was refused for the wrong reason:\n%s\n' "$what" "$output" >&2
		exit 1
	fi
}

expect_acceptance() {
	if ! (cd "$work_dir" && ./scripts/check-action-publishable.sh >/dev/null 2>&1); then
		printf 'check-action-publishable-selftest: the repaired tree was refused\n' >&2
		exit 1
	fi
}

write_valid
expect_acceptance

# A file the published repository needs, missing.
rm "$action_dir/LICENSE"
git -C "$work_dir" add -A
expect_refusal "a missing LICENSE" "LICENSE is missing"
write_valid

# A script the action names but does not ship.
sed -i.bak 's|/run\.sh|/runner.sh|' "$action_dir/action.yml" && rm -f "$action_dir/action.yml.bak"
git -C "$work_dir" add -A
expect_refusal "a script that is not shipped" "runner.sh, which is not in the directory"
write_valid

# A path that leaves the directory, which exists here and not once published.
printf '\ncat ../../schema.sql\n' >>"$action_dir/run.sh"
git -C "$work_dir" add -A
expect_refusal "a path outside the directory" "refers outside"
write_valid

# A README that does not tell an editor where the file lives.
printf 'Ptah Action\n' >"$action_dir/README.md"
git -C "$work_dir" add -A
expect_refusal "a README with no provenance" "does not say it is published from here"
write_valid
expect_acceptance

printf 'check-action-publishable-selftest: OK (4 refusals and 2 acceptances)\n'

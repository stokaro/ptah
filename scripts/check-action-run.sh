#!/usr/bin/env bash
# Exercises .github/actions/ptah/run.sh with a recording stand-in for the ptah
# binary, and holds what the script passes to each command it runs.
#
# The property held here is one a workflow author cannot see: an input reaches
# every command that reads it. `allow-value-rounding` has to reach both
# `migrations plan` runs and `migrations generate`, because each of them reads
# the rows a rounding change touches and refuses on its own, and it has to stay
# away from `migrations lint`, which takes no such flag. The flag reads no
# environment variable, so an input the script forgets to pass is a run that
# fails for a reason the workflow cannot fix.
#
# PTAH_ACTION_RUN_SCRIPT aims the check at another copy of the script, which is
# how check-action-run-selftest.sh proves this check can fail.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

script="${PTAH_ACTION_RUN_SCRIPT:-$repo_root/.github/actions/ptah/run.sh}"

command -v node >/dev/null 2>&1 || {
	echo "check-action-run: need node" >&2
	exit 1
}

work="$(mktemp -d "${TMPDIR:-/tmp}/ptah-action-run-check.XXXXXX")"
trap 'rm -rf "$work"' EXIT INT TERM

# The stand-in writes one line per invocation, its arguments separated by a
# tab, and succeeds. It writes the JSON the safety parse expects, so the script
# runs to its end.
fake="$work/ptah"
cat >"$fake" <<'FAKE'
#!/usr/bin/env bash
(IFS=$'\t'; printf '%s\n' "$*") >>"$PTAH_ACTION_RUN_LOG"
printf '{"destructive":false}\n'
FAKE
chmod +x "$fake"

failures=0
checks=0
fail() {
	failures=$((failures + 1))
	printf '  FAIL  %s\n' "$*" >&2
}

# run_case runs the script with the inputs given as NAME=value arguments and
# leaves the invocations in $work/<case>.log.
run_case() {
	local name="$1"
	shift
	local log="$work/$name.log" output="$work/$name.output" migrations="$work/$name-migrations"
	: >"$log"
	: >"$output"
	mkdir -p "$migrations"
	if ! env PTAH_BIN="$fake" PTAH_ACTION_RUN_LOG="$log" GITHUB_OUTPUT="$output" GITHUB_STEP_SUMMARY= \
		INPUT_DB_URL="postgres://check@127.0.0.1/check" INPUT_DIR="." INPUT_MIGRATION_DIR="$migrations" \
		INPUT_OUTPUT_DIR="$work/$name-reports" INPUT_GENERATE=true INPUT_LINT=true "$@" \
		bash "$script" >"$work/$name.stdout" 2>&1; then
		fail "$name: the script exited non-zero"
		sed 's/^/    | /' "$work/$name.stdout" >&2
	fi
}

# count_with prints how many invocations of a command in a log carry a flag.
count_with() {
	local log="$1" command="$2" flag="$3"
	awk -F '\t' -v command="$command" -v flag="$flag" '
		$1 " " $2 == command { for (i = 3; i <= NF; i++) if ($i == flag) { n++; break } }
		END { print n + 0 }
	' "$log"
}

# count_runs prints how many invocations of a command a log holds.
count_runs() {
	awk -F '\t' -v command="$2" '$1 " " $2 == command { n++ } END { print n + 0 }' "$1"
}

expect() {
	local description="$1" want="$2" got="$3"
	checks=$((checks + 1))
	if [ "$want" != "$got" ]; then
		fail "$description: want $want, got $got"
	fi
}

run_case allowed INPUT_ALLOW_VALUE_ROUNDING=true
run_case default

# The floor: each case ran every command once, and a stand-in that stopped
# recording reads as zero invocations rather than as a pass.
for name in allowed default; do
	expect "$name: migrations plan runs" 2 "$(count_runs "$work/$name.log" "migrations plan")"
	expect "$name: migrations generate runs" 1 "$(count_runs "$work/$name.log" "migrations generate")"
	expect "$name: migrations lint runs" 1 "$(count_runs "$work/$name.log" "migrations lint")"
done

expect "allow-value-rounding reaches both plans" 2 \
	"$(count_with "$work/allowed.log" "migrations plan" --allow-value-rounding)"
expect "allow-value-rounding reaches generate" 1 \
	"$(count_with "$work/allowed.log" "migrations generate" --allow-value-rounding)"
expect "allow-value-rounding stays away from lint" 0 \
	"$(count_with "$work/allowed.log" "migrations lint" --allow-value-rounding)"
expect "no input passes no --allow-value-rounding to plan" 0 \
	"$(count_with "$work/default.log" "migrations plan" --allow-value-rounding)"
expect "no input passes no --allow-value-rounding to generate" 0 \
	"$(count_with "$work/default.log" "migrations generate" --allow-value-rounding)"

if [ "$failures" -ne 0 ]; then
	echo "check-action-run: $failures of $checks checks failed" >&2
	exit 1
fi
echo "check-action-run: OK ($checks checks)"

#!/usr/bin/env bash
# Proves that scripts/check-readme-example.sh can still fail.
#
# A gate that runs the README's example is only worth its runtime if it goes red
# when the README stops being true. That is not observable from a green run: a
# pattern that matched nothing, a command whose output nobody compared, and a
# healthy tree all print the same success.
#
# So each case below writes a fixture README that breaks the gate's rule in one
# way and requires a non-zero exit, with a faithful fixture first as the control
# -- without it, a gate that failed on everything would look like a gate that
# reads.
#
# The fixtures run against a stub `ptah` rather than a build. What is under test
# here is the gate's reading of the README, and a stub makes each case a second
# rather than a compile. The real binary is what the gate itself uses.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-readme-example-selftest.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

# The stub answers the two commands the fixtures use, and nothing else. It is a
# real program on PATH because the gate runs the command line the README prints,
# through a shell, exactly as a reader would.
mkdir -p "$work_dir/bin"
cat >"$work_dir/bin/ptah" <<'STUB'
#!/usr/bin/env bash
case "${2-}" in
render)
	echo "-- Statement 1/1"
	echo 'CREATE TABLE "users" ('
	echo ');'
	;;
apply)
	echo "Schema apply completed successfully."
	;;
drift)
	echo "No schema drift detected."
	;;
explode)
	echo "boom" >&2
	exit 7
	;;
*)
	echo "stub: unexpected ${*}" >&2
	exit 64
	;;
esac
STUB
chmod +x "$work_dir/bin/ptah"

# fixture writes a README holding one section, from the three parts every case
# varies: the schema block, the steps, and anything appended after them.
fixture() {
	local path="$1" steps="$2"
	mkdir -p "$(dirname "$path")"
	{
		printf '# Fixture\n\n## Run it end to end\n\n'
		printf 'Save the schema you want as `schema.sql`:\n\n'
		printf '```sql\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n```\n\n'
		printf '%s' "$steps"
		printf '\n## After\n\nNothing here.\n'
	} >"$path"
}

# step renders one command block and, when given a third argument, the expected
# output block the gate has to compare against.
step() {
	local heading="$1" command="$2" expected="${3-}"
	printf '### %s\n\n```bash\n%s\n```\n\n' "$heading" "$command"
	if [ -n "$expected" ]; then
		printf 'Expected output includes:\n\n```text\n%s\n```\n\n' "$expected"
	fi
}

faithful_steps="$(
	step "See the SQL" "ptah schema render" "-- Statement 1/1"
	step "Apply it" "ptah schema apply" "Schema apply completed successfully."
	step "Check for drift" "ptah schema drift" "No schema drift detected."
)"

failures=0
checked=0

run_case() {
	local description="$1" path="$2" expectation="$3"
	checked=$((checked + 1))

	local status=0
	README_EXAMPLE_FILE="$path" README_EXAMPLE_BIN="$work_dir/bin/ptah" \
		bash scripts/check-readme-example.sh >"$work_dir/last.log" 2>&1 || status=$?

	if [ "$expectation" = "passes" ] && [ "$status" -ne 0 ]; then
		echo "check-readme-example-selftest: the faithful fixture FAILED; every case below proves nothing" >&2
		cat "$work_dir/last.log" >&2
		failures=$((failures + 1))
		return
	fi
	if [ "$expectation" = "fails" ] && [ "$status" -eq 0 ]; then
		echo "check-readme-example-selftest: PASSED with ${description}" >&2
		cat "$work_dir/last.log" >&2
		failures=$((failures + 1))
		return
	fi
	printf '  %-56s %s\n' "$description" "$expectation"
}

echo "check-readme-example-selftest: breaking the README's example and requiring the gate to notice"

fixture "$work_dir/faithful/README.md" "$faithful_steps"
run_case "a README whose example is true" "$work_dir/faithful/README.md" passes

fixture "$work_dir/wrong-output/README.md" "$(
	step "See the SQL" "ptah schema render" "-- Statement 1/1"
	step "Apply it" "ptah schema apply" "Schema apply completed successfully."
	step "Check for drift" "ptah schema drift" "Drift detected in 4 tables."
)"
run_case "output the command never prints" "$work_dir/wrong-output/README.md" fails

fixture "$work_dir/out-of-order/README.md" "$(
	step "See the SQL" "ptah schema render" "$(printf ');\n-- Statement 1/1')"
	step "Apply it" "ptah schema apply" "Schema apply completed successfully."
	step "Check for drift" "ptah schema drift" "No schema drift detected."
)"
run_case "the promised lines printed in the other order" "$work_dir/out-of-order/README.md" fails

fixture "$work_dir/failing-command/README.md" "$(
	step "See the SQL" "ptah schema render" "-- Statement 1/1"
	step "Apply it" "ptah schema explode" "Schema apply completed successfully."
	step "Check for drift" "ptah schema drift" "No schema drift detected."
)"
run_case "a command that exits non-zero" "$work_dir/failing-command/README.md" fails

fixture "$work_dir/too-few/README.md" "$(
	step "See the SQL" "ptah schema render" "-- Statement 1/1"
)"
run_case "an example shrunk below the command floor" "$work_dir/too-few/README.md" fails

fixture "$work_dir/unchecked/README.md" "$(
	step "See the SQL" "ptah schema render"
	step "Apply it" "ptah schema apply"
	step "Check for drift" "ptah schema drift"
)"
run_case "commands nobody states an output for" "$work_dir/unchecked/README.md" fails

# The section heading is how the gate finds anything at all. A renamed heading
# must be a failure rather than an empty, successful run.
printf '# Fixture\n\n## Somewhere else\n\nNothing here.\n' >"$work_dir/no-section-README.md"
run_case "the section renamed out from under the gate" "$work_dir/no-section-README.md" fails

echo
if [ "$failures" -ne 0 ]; then
	echo "check-readme-example-selftest: ${failures} of ${checked} case(s) went the wrong way" >&2
	exit 1
fi
echo "check-readme-example-selftest: OK (${checked} cases; the gate reads the README it is given)"

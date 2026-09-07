#!/usr/bin/env bash
set -euo pipefail

ptah_bin="${PTAH_BIN:?PTAH_BIN is required}"
output_dir="${INPUT_OUTPUT_DIR:-}"
if [[ -z "$output_dir" ]]; then
	output_dir="$(mktemp -d)"
else
	mkdir -p "$output_dir"
fi

plan_path="$output_dir/ptah-plan.txt"
safety_path="$output_dir/ptah-safety.json"
safety_error_path="$output_dir/ptah-safety.stderr.txt"
lint_path="$output_dir/ptah-lint.json"
lint_error_path="$output_dir/ptah-lint.stderr.txt"

# The desired schema is forwarded, never reinterpreted: each selector maps to
# the flag Ptah already has, and Ptah decides what a source means.
#
# `dir` used to default to `.`, so a run selecting a SQL file still scanned the
# working directory for Go entities and silently composed a schema nobody asked
# for. It defaults to empty now, and the fallback below keeps the old behaviour
# for a run that selects nothing at all.
read_lines() {
	printf '%s\n' "$1" | while IFS= read -r line; do
		line="${line#"${line%%[![:space:]]*}"}"
		line="${line%"${line##*[![:space:]]}"}"
		[[ -n "$line" ]] && printf '%s\n' "$line"
	done
}

if [[ -n "${INPUT_SCHEMA_FORMAT:-}" && -z "${INPUT_SCHEMA_CMD:-}" ]]; then
	echo "schema-format selects the output format of schema-cmd, which is not set" >&2
	exit 2
fi

source_args=()
while IFS= read -r root; do
	source_args+=(--root-dir "$root")
done < <(read_lines "${INPUT_DIR:-}")
while IFS= read -r file; do
	source_args+=(--schema-file "$file")
done < <(read_lines "${INPUT_SCHEMA_FILE:-}")
if [[ -n "${INPUT_SCHEMA_CMD:-}" ]]; then
	source_args+=(--schema-cmd "$INPUT_SCHEMA_CMD")
	if [[ -n "${INPUT_SCHEMA_FORMAT:-}" ]]; then
		source_args+=(--schema-format "$INPUT_SCHEMA_FORMAT")
	fi
fi
if [[ "${#source_args[@]}" -eq 0 ]]; then
	source_args=(--root-dir .)
fi

common_args=(migrations plan "${source_args[@]}" --db-url "${INPUT_DB_URL:?db-url input is required}")
if [[ -n "${INPUT_SCHEMAS:-}" ]]; then
	common_args+=(--schemas "$INPUT_SCHEMAS")
fi

set +e
"$ptah_bin" "${common_args[@]}" --report text >"$plan_path" 2>&1
plan_status="$?"

safety_args=("${common_args[@]}" --report json --check-destructive)
if [[ "${INPUT_ALLOW_DESTRUCTIVE:-false}" == "true" ]]; then
	safety_args+=(--allow-destructive)
fi
"$ptah_bin" "${safety_args[@]}" >"$safety_path" 2>"$safety_error_path"
safety_status="$?"

generate_status="0"
generated_list_path="$output_dir/ptah-generated.txt"
: >"$generated_list_path"
if [[ "${INPUT_GENERATE:-false}" == "true" ]]; then
	migration_dir="${INPUT_MIGRATION_DIR:-migrations}"
	mkdir -p "$migration_dir"
	# The directory is compared before and after rather than parsing the
	# command's prose: the file names carry a timestamp the caller cannot
	# predict, and a message format is a worse contract than the filesystem.
	before="$(mktemp)"
	after="$(mktemp)"
	find "$migration_dir" -type f -name '*.sql' | LC_ALL=C sort >"$before"
	generate_args=(migrations generate "${source_args[@]}" \
		--db-url "$INPUT_DB_URL" --migrations-dir "$migration_dir")
	if [[ -n "${INPUT_GENERATE_NAME:-}" ]]; then
		generate_args+=(--name "$INPUT_GENERATE_NAME")
	fi
	"$ptah_bin" "${generate_args[@]}" >"$output_dir/ptah-generate.txt" 2>&1
	generate_status="$?"
	find "$migration_dir" -type f -name '*.sql' | LC_ALL=C sort >"$after"
	comm -13 "$before" "$after" >"$generated_list_path"
	rm -f "$before" "$after"
fi

lint_status="0"
if [[ "${INPUT_LINT:-true}" == "true" ]]; then
	lint_args=(migrations lint --dir "${INPUT_MIGRATION_DIR:-migrations}" --format json --fail-on "${INPUT_LINT_FAIL_ON:-error}")
	if [[ -n "${INPUT_DIALECT:-}" ]]; then
		lint_args+=(--dialect "$INPUT_DIALECT")
	fi
	"$ptah_bin" "${lint_args[@]}" >"$lint_path" 2>"$lint_error_path"
	lint_status="$?"
else
	printf '{"failed":false,"findings":[]}\n' >"$lint_path"
	: >"$lint_error_path"
fi
set -e

destructive="$(
	node - "$safety_path" <<'NODE'
const fs = require("fs");
try {
  const report = JSON.parse(fs.readFileSync(process.argv[2], "utf8"));
  console.log(report.destructive ? "true" : "false");
} catch {
  console.log("unknown");
}
NODE
)"

{
	printf 'plan-path=%s\n' "$plan_path"
	printf 'safety-path=%s\n' "$safety_path"
	printf 'safety-error-path=%s\n' "$safety_error_path"
	printf 'lint-path=%s\n' "$lint_path"
	printf 'lint-error-path=%s\n' "$lint_error_path"
	printf 'plan-exit-code=%s\n' "$plan_status"
	printf 'safety-exit-code=%s\n' "$safety_status"
	printf 'lint-exit-code=%s\n' "$lint_status"
	printf 'destructive=%s\n' "$destructive"
	printf 'generate-exit-code=%s\n' "$generate_status"
	printf 'generated-list-path=%s\n' "$generated_list_path"
	printf 'generated-count=%s\n' "$(wc -l <"$generated_list_path" | tr -d ' ')"
} >>"$GITHUB_OUTPUT"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
	{
		printf '## Ptah migration plan\n\n'
		printf '| Check | Result |\n'
		printf '| --- | --- |\n'
		printf '| Plan | exit %s |\n' "$plan_status"
		printf '| Safety | exit %s, destructive: %s |\n' "$safety_status" "$destructive"
		printf '| Lint | exit %s |\n' "$lint_status"
		if [[ "${INPUT_GENERATE:-false}" == "true" ]]; then
			printf '| Generate | exit %s, %s file(s) |\n' \
				"$generate_status" "$(wc -l <"$generated_list_path" | tr -d ' ')"
		fi
	} >>"$GITHUB_STEP_SUMMARY"
fi

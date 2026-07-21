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

common_args=(migrations plan --root-dir "${INPUT_DIR:-.}" --db-url "${INPUT_DB_URL:?db-url input is required}")
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
} >>"$GITHUB_OUTPUT"

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
	{
		printf '## Ptah migration plan\n\n'
		printf '| Check | Result |\n'
		printf '| --- | --- |\n'
		printf '| Plan | exit %s |\n' "$plan_status"
		printf '| Safety | exit %s, destructive: %s |\n' "$safety_status" "$destructive"
		printf '| Lint | exit %s |\n' "$lint_status"
	} >>"$GITHUB_STEP_SUMMARY"
fi

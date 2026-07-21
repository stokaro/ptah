#!/usr/bin/env bash
set -euo pipefail

export GOWORK=off

baseline_ref="${PTAH_PUBLIC_API_BASELINE_REF:-}"
require_baseline=0

while [[ "$#" -gt 0 ]]; do
	case "$1" in
		--baseline-ref)
			if [[ "$#" -lt 2 ]]; then
				printf 'missing value for --baseline-ref\n' >&2
				exit 2
			fi
			baseline_ref="$2"
			shift 2
			;;
		--require-baseline)
			require_baseline=1
			shift
			;;
		*)
			printf 'unknown argument: %s\n' "$1" >&2
			exit 2
			;;
	esac
done

module_path="$(go list -m -f '{{.Path}}')"
packages="$(mktemp)"
approvals="docs/public_api_approvals.txt"
baseline_dir="$(mktemp -d)"
exports_dir="$(mktemp -d)"
trap 'rm -f "$packages"; rm -rf "$baseline_dir" "$exports_dir"' EXIT

grep -Eo "\`github\.com/stokaro/ptah[^\`]+\`" docs/public_api.md |
	tr -d '`' |
	sort -u >"$packages"

if [[ -z "$baseline_ref" ]]; then
	baseline_ref="$(git tag --list 'v0.*' --sort=-v:refname | head -n 1)"
fi

if [[ -z "$baseline_ref" ]]; then
	printf 'No v0.x tag found; released-baseline public API check skipped.\n' >&2
	printf 'Run with --require-baseline after the first v0.x release tag exists.\n' >&2
	if [[ "$require_baseline" -eq 1 ]]; then
		exit 1
	fi
	exit 0
fi

if ! git rev-parse --verify --quiet "${baseline_ref}^{commit}" >/dev/null; then
	printf 'public API baseline ref does not resolve to a commit: %s\n' "$baseline_ref" >&2
	exit 1
fi

apidiff_bin="$(go tool -n apidiff)"
git archive "$baseline_ref" | tar -x -C "$baseline_dir"

baseline_packages="$(mktemp)"
trap 'rm -f "$packages" "$baseline_packages"; rm -rf "$baseline_dir" "$exports_dir"' EXIT

(
	cd "$baseline_dir"
	go list ./...
) | sort -u >"$baseline_packages"

status=0
while IFS= read -r package_path; do
	if ! grep -Fxq "$package_path" "$baseline_packages"; then
		printf 'public API baseline %s has no package %s; treating it as a new stable package\n' \
			"$baseline_ref" "$package_path" >&2
		continue
	fi

	relative_package="${package_path#"$module_path"/}"
	export_file="$exports_dir/${relative_package//\//__}.export"

	(
		cd "$baseline_dir"
		"$apidiff_bin" -w "$export_file" "$package_path"
	)

	diff_output="$("$apidiff_bin" -incompatible "$export_file" "$package_path" 2>&1)" || {
		printf '%s\n' "$diff_output" >&2
		status=1
		continue
	}

	if [[ -z "$diff_output" ]]; then
		continue
	fi

	printf '%s\n' "$diff_output"
	if awk -v baseline="$baseline_ref" -v package="$package_path" \
		'$1 == baseline && $2 == package { found = 1 } END { exit !found }' "$approvals"; then
		printf 'approved pre-v1 public API incompatibility: %s against %s\n' \
			"$package_path" "$baseline_ref" >&2
	else
		status=1
	fi
done <"$packages"

if [[ "$status" -ne 0 ]]; then
	printf '\nPublic API incompatibilities detected against baseline %s.\n' "$baseline_ref" >&2
	printf 'If the change is intentional before v1, add a reviewed approval in docs/public_api_approvals.txt and explain the rationale in the PR.\n' >&2
fi

exit "$status"

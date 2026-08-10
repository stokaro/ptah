#!/usr/bin/env bash

# Shared path resolution, lock-file loading, and binary validation for scripts
# that measure Atlas CE as an external black-box oracle. Call
# atlas_ce_load_lock before the version functions.

atlas_ce_resolve_binary() {
	local argument_path="${1:-}"

	if [[ -n "$argument_path" ]]; then
		printf '%s\n' "$argument_path"
		return 0
	fi
	if [[ -n "${PTAH_ATLAS_ORACLE:-}" ]]; then
		printf '%s\n' "$PTAH_ATLAS_ORACLE"
		return 0
	fi

	printf 'atlas-ce: oracle path required; pass it as an argument or set PTAH_ATLAS_ORACLE\n' >&2
	return 1
}

atlas_ce_load_lock() {
	local lock_file="$1"
	local key value extra

	ATLAS_CE_VERSION=""
	ATLAS_CE_SOURCE_COMMIT=""
	ATLAS_CE_SOURCE_URL=""
	ATLAS_CE_SOURCE_SHA256=""

	if [[ ! -f "$lock_file" ]]; then
		printf 'atlas-ce: lock file not found: %s\n' "$lock_file" >&2
		return 1
	fi

	while read -r key value extra; do
		case "$key" in
		"" | \#*)
			continue
			;;
		version)
			if [[ -n "$ATLAS_CE_VERSION" ]]; then
				printf 'atlas-ce: duplicate version in %s\n' "$lock_file" >&2
				return 1
			fi
			ATLAS_CE_VERSION="$value"
			;;
		source_commit)
			if [[ -n "$ATLAS_CE_SOURCE_COMMIT" ]]; then
				printf 'atlas-ce: duplicate source_commit in %s\n' "$lock_file" >&2
				return 1
			fi
			ATLAS_CE_SOURCE_COMMIT="$value"
			;;
		source_url)
			if [[ -n "$ATLAS_CE_SOURCE_URL" ]]; then
				printf 'atlas-ce: duplicate source_url in %s\n' "$lock_file" >&2
				return 1
			fi
			ATLAS_CE_SOURCE_URL="$value"
			;;
		source_sha256)
			if [[ -n "$ATLAS_CE_SOURCE_SHA256" ]]; then
				printf 'atlas-ce: duplicate source_sha256 in %s\n' "$lock_file" >&2
				return 1
			fi
			ATLAS_CE_SOURCE_SHA256="$value"
			;;
		*)
			printf 'atlas-ce: unknown lock key %q in %s\n' "$key" "$lock_file" >&2
			return 1
			;;
		esac

		if [[ -n "$extra" || -z "$value" ]]; then
			printf 'atlas-ce: malformed %s entry in %s\n' "$key" "$lock_file" >&2
			return 1
		fi
	done <"$lock_file"

	if [[ ! "$ATLAS_CE_VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
		printf 'atlas-ce: invalid version %q in %s\n' "$ATLAS_CE_VERSION" "$lock_file" >&2
		return 1
	fi
	if [[ ! "$ATLAS_CE_SOURCE_COMMIT" =~ ^[0-9a-f]{40}$ ]]; then
		printf 'atlas-ce: invalid source_commit %q in %s\n' "$ATLAS_CE_SOURCE_COMMIT" "$lock_file" >&2
		return 1
	fi
	if [[ ! "$ATLAS_CE_SOURCE_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
		printf 'atlas-ce: invalid source_sha256 %q in %s\n' "$ATLAS_CE_SOURCE_SHA256" "$lock_file" >&2
		return 1
	fi

	local expected_url="https://codeload.github.com/ariga/atlas/tar.gz/$ATLAS_CE_SOURCE_COMMIT"
	if [[ "$ATLAS_CE_SOURCE_URL" != "$expected_url" ]]; then
		printf 'atlas-ce: source_url must address source_commit exactly\n' >&2
		printf '  want: %s\n' "$expected_url" >&2
		printf '  got:  %s\n' "$ATLAS_CE_SOURCE_URL" >&2
		return 1
	fi
}

atlas_ce_expected_version() {
	printf 'atlas community version %s\n' "$ATLAS_CE_VERSION"
}

atlas_ce_verify_binary() {
	local binary="$1"
	local actual_version expected_version

	if [[ ! -x "$binary" ]]; then
		printf 'atlas-ce: oracle not found or not executable: %s\n' "$binary" >&2
		return 1
	fi

	if ! actual_version="$("$binary" version)"; then
		printf 'atlas-ce: oracle version command failed: %s\n' "$binary" >&2
		return 1
	fi
	actual_version="${actual_version%%$'\n'*}"
	actual_version="${actual_version%$'\r'}"
	expected_version="$(atlas_ce_expected_version)"

	if [[ "$actual_version" != "$expected_version" ]]; then
		printf 'atlas-ce: oracle version mismatch\n' >&2
		printf '  want: %s\n' "$expected_version" >&2
		printf '  got:  %s\n' "$actual_version" >&2
		return 1
	fi

	printf '%s\n' "$actual_version"
}

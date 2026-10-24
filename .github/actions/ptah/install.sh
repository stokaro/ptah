#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${PTAH_BINARY_PATH:-}" ]]; then
	ptah_dir="$(cd "$(dirname "$PTAH_BINARY_PATH")" && pwd)"
	ptah_bin="$ptah_dir/$(basename "$PTAH_BINARY_PATH")"
	if [[ ! -x "$ptah_bin" ]]; then
		printf 'Ptah binary is not executable: %s\n' "$ptah_bin" >&2
		exit 1
	fi
	printf 'ptah-bin=%s\n' "$ptah_bin" >>"$GITHUB_OUTPUT"
	exit 0
fi

version="${PTAH_VERSION:-latest}"
os="$(uname -s | tr '[:upper:]' '[:lower:]')"
arch="$(uname -m)"
case "$arch" in
	x86_64) arch="amd64" ;;
	aarch64 | arm64) arch="arm64" ;;
	*)
		printf 'unsupported runner architecture: %s\n' "$arch" >&2
		exit 1
		;;
esac

case "$os" in
	linux | darwin) ;;
	*)
		printf 'unsupported runner OS: %s\n' "$os" >&2
		exit 1
		;;
esac

tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT
install_dir="${RUNNER_TEMP:-$tmp_dir}/ptah-action-bin"

install_from_source() {
	local ref="$version"
	if [[ "$ref" == "latest" ]]; then
		ref="master"
	fi
	printf 'No Ptah release asset found for %s; installing from source at %s.\n' "$version" "$ref" >&2
	GOPROXY=direct GOBIN="$install_dir" go install "ptah.run/cmd/ptah@$ref"
	chmod +x "$install_dir/ptah"
	printf '%s\n' "$install_dir" >>"$GITHUB_PATH"
	printf 'ptah-bin=%s\n' "$install_dir/ptah" >>"$GITHUB_OUTPUT"
}
mkdir -p "$install_dir"

api_url="${PTAH_ACTION_API_URL:-https://api.github.com/repos/stokaro/ptah}"
if [[ "$version" == "latest" ]]; then
	release_url="$api_url/releases/latest"
else
	release_url="$api_url/releases/tags/$version"
fi

curl_args=(-fsSL)
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
	curl_args+=(-H "Authorization: Bearer $GITHUB_TOKEN")
fi
if ! curl "${curl_args[@]}" "$release_url" -o "$tmp_dir/release.json"; then
	install_from_source
	exit 0
fi

# Exit 2 means the release has no archive for this runner, which is the one case
# that builds from source. Exit 3 means it has an archive and no checksums.txt:
# that is a refusal, because falling back to source would turn a missing
# checksum file into a different, unreviewed install.
selection_status=0
selection="$(
	node "$GITHUB_ACTION_PATH/select-asset.js" "$tmp_dir/release.json" "$os" "$arch"
)" || selection_status=$?
case "$selection_status" in
	0) ;;
	2)
		install_from_source
		exit 0
		;;
	*)
		printf 'refusing to install: the release has an archive for %s/%s but no checksums.txt to verify it\n' "$os" "$arch" >&2
		exit 1
		;;
esac
asset_name="$(printf '%s\n' "$selection" | sed -n 1p)"
asset_url="$(printf '%s\n' "$selection" | sed -n 2p)"
checksums_url="$(printf '%s\n' "$selection" | sed -n 3p)"

sha256_of() {
	if command -v sha256sum >/dev/null 2>&1; then
		sha256sum "$1" | awk '{print $1}'
	elif command -v shasum >/dev/null 2>&1; then
		shasum -a 256 "$1" | awk '{print $1}'
	else
		printf 'need sha256sum or shasum to verify the Ptah archive\n' >&2
		return 1
	fi
}

archive="$tmp_dir/$asset_name"
checksums="$tmp_dir/checksums.txt"
curl "${curl_args[@]}" "$asset_url" -o "$archive"
curl "${curl_args[@]}" "$checksums_url" -o "$checksums"

# Verify before anything is unpacked, and refuse on every way the check can fail
# to happen: no line for the archive, an empty digest, a missing hash tool, a
# different digest. The name is matched as a whole field, so the archive's own
# .sbom.json line cannot stand in for it.
want="$(awk -v name="$asset_name" '$2 == name { print $1; exit }' "$checksums")"
if [[ -z "$want" ]]; then
	printf 'refusing to install: checksums.txt has no entry for %s\n' "$asset_name" >&2
	exit 1
fi
got="$(sha256_of "$archive")"
if [[ -z "$got" ]]; then
	printf 'refusing to install: could not compute the SHA-256 of %s\n' "$asset_name" >&2
	exit 1
fi
if [[ "$want" != "$got" ]]; then
	printf 'refusing to install: checksum mismatch for %s\n  want %s\n  got  %s\n' "$asset_name" "$want" "$got" >&2
	exit 1
fi

unpack_dir="$tmp_dir/unpack"
mkdir -p "$unpack_dir"
tar -xzf "$archive" -C "$unpack_dir"

ptah_bin="$unpack_dir/ptah"
if [[ ! -x "$ptah_bin" ]]; then
	printf 'downloaded archive did not contain an executable ptah binary\n' >&2
	exit 1
fi

cp "$ptah_bin" "$install_dir/ptah"
chmod +x "$install_dir/ptah"

printf '%s\n' "$install_dir" >>"$GITHUB_PATH"
printf 'ptah-bin=%s\n' "$install_dir/ptah" >>"$GITHUB_OUTPUT"

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
	GOBIN="$install_dir" go install "github.com/stokaro/ptah/cmd/ptah@$ref"
	chmod +x "$install_dir/ptah"
	printf '%s\n' "$install_dir" >>"$GITHUB_PATH"
	printf 'ptah-bin=%s\n' "$install_dir/ptah" >>"$GITHUB_OUTPUT"
}
mkdir -p "$install_dir"

if [[ "$version" == "latest" ]]; then
	release_url="https://api.github.com/repos/stokaro/ptah/releases/latest"
else
	release_url="https://api.github.com/repos/stokaro/ptah/releases/tags/$version"
fi

curl_args=(-fsSL)
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
	curl_args+=(-H "Authorization: Bearer $GITHUB_TOKEN")
fi
if ! curl "${curl_args[@]}" "$release_url" -o "$tmp_dir/release.json"; then
	install_from_source
	exit 0
fi

asset_url="$(
	node "$GITHUB_ACTION_PATH/select-asset.js" "$tmp_dir/release.json" "$os" "$arch"
)" || {
	install_from_source
	exit 0
}
archive="$tmp_dir/ptah.tar.gz"
curl "${curl_args[@]}" "$asset_url" -o "$archive"
tar -xzf "$archive" -C "$tmp_dir"

ptah_bin="$tmp_dir/ptah"
if [[ ! -x "$ptah_bin" ]]; then
	printf 'downloaded archive did not contain an executable ptah binary\n' >&2
	exit 1
fi

cp "$ptah_bin" "$install_dir/ptah"
chmod +x "$install_dir/ptah"

printf '%s\n' "$install_dir" >>"$GITHUB_PATH"
printf 'ptah-bin=%s\n' "$install_dir/ptah" >>"$GITHUB_OUTPUT"

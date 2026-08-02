#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# shellcheck source=scripts/lib/atlas-ce-oracle.sh
source "$SCRIPT_DIR/lib/atlas-ce-oracle.sh"
atlas_ce_load_lock "$SCRIPT_DIR/atlas-ce-oracle.lock"

if [[ $# -gt 1 ]]; then
	printf 'usage: %s [output-path]\n' "$0" >&2
	exit 2
fi

output="${1:-$REPO_ROOT/bin/atlas-ce-oracle}"
mkdir -p "$(dirname "$output")"
output_dir="$(cd "$(dirname "$output")" && pwd)"
output="$output_dir/$(basename "$output")"

workspace="$(mktemp -d "$output_dir/.atlas-ce-build.XXXXXX")"
trap 'rm -rf "$workspace"' EXIT

archive="$workspace/atlas-source.tar.gz"
source_dir="$workspace/source"
candidate="$workspace/atlas"

printf 'atlas-ce: resolving tag %s\n' "$ATLAS_CE_VERSION"
if tag_ref="$(git ls-remote --exit-code https://github.com/ariga/atlas.git \
	"refs/tags/$ATLAS_CE_VERSION^{}" 2>/dev/null)"; then
	:
else
	tag_ref="$(git ls-remote --exit-code https://github.com/ariga/atlas.git \
		"refs/tags/$ATLAS_CE_VERSION")"
fi
tag_commit="${tag_ref%%$'\t'*}"
if [[ "$tag_commit" != "$ATLAS_CE_SOURCE_COMMIT" ]]; then
	printf 'atlas-ce: release tag commit mismatch\n' >&2
	printf '  want: %s\n' "$ATLAS_CE_SOURCE_COMMIT" >&2
	printf '  got:  %s\n' "$tag_commit" >&2
	exit 1
fi

printf 'atlas-ce: downloading %s at %s\n' "$ATLAS_CE_VERSION" "$ATLAS_CE_SOURCE_COMMIT"
curl --fail --location --proto '=https' --proto-redir '=https' --tlsv1.2 --retry 3 \
	--silent --show-error --output "$archive" "$ATLAS_CE_SOURCE_URL"

if command -v sha256sum >/dev/null 2>&1; then
	actual_sha256="$(sha256sum "$archive")"
	actual_sha256="${actual_sha256%% *}"
elif command -v shasum >/dev/null 2>&1; then
	actual_sha256="$(shasum -a 256 "$archive")"
	actual_sha256="${actual_sha256%% *}"
else
	printf 'atlas-ce: sha256sum or shasum is required\n' >&2
	exit 1
fi

if [[ "$actual_sha256" != "$ATLAS_CE_SOURCE_SHA256" ]]; then
	printf 'atlas-ce: source archive checksum mismatch\n' >&2
	printf '  want: %s\n' "$ATLAS_CE_SOURCE_SHA256" >&2
	printf '  got:  %s\n' "$actual_sha256" >&2
	exit 1
fi

mkdir -p "$source_dir"
tar -xzf "$archive" --strip-components=1 -C "$source_dir"
if [[ ! -f "$source_dir/go.mod" || ! -d "$source_dir/cmd/atlas" ]]; then
	printf 'atlas-ce: verified source archive has an unexpected layout\n' >&2
	exit 1
fi

printf 'atlas-ce: building verified source archive\n'
(
	cd "$source_dir/cmd/atlas"
	GOWORK=off go build -trimpath \
		-ldflags "-X ariga.io/atlas/cmd/atlas/internal/cmdapi.version=$ATLAS_CE_VERSION" \
		-o "$candidate" .
)

atlas_ce_verify_binary "$candidate" >/dev/null
chmod 0755 "$candidate"
mv -f "$candidate" "$output"

printf 'atlas-ce: installed verified oracle at %s\n' "$output"
atlas_ce_verify_binary "$output"

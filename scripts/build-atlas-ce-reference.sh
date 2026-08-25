#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# shellcheck source=scripts/lib/atlas-ce-reference.sh
source "$SCRIPT_DIR/lib/atlas-ce-reference.sh"
atlas_ce_load_lock "$SCRIPT_DIR/atlas-ce-reference.lock"

if [[ $# -gt 1 ]]; then
	printf 'usage: %s [output-path]\n' "$0" >&2
	exit 2
fi

output="${1:-$REPO_ROOT/bin/atlas-ce-reference}"
mkdir -p "$(dirname "$output")"
output_dir="$(cd "$(dirname "$output")" && pwd)"
output="$output_dir/$(basename "$output")"

# What the binary beside this stamp was built from. Reuse is decided on the
# SOURCE checksum rather than on the version alone: two builds of one tag report
# the same `atlas version`, so a stamp holding only the version would accept a
# binary built from a different archive after the lock moved
# (stokaro/ptah#2186).
stamp="$output.source-sha256"

# A binary already here that matches the lock is the one this script would spend
# 1m40s producing. Measured on run 32820538377: the build is 1m40s of a
# 22.6-minute integration job, repeated on every run, for a source archive
# pinned by checksum.
#
# The verification still runs, and that is why this lives here rather than only
# in the workflow. A cache that restores a file and trusts it because a key
# matched removes the one check that makes the binary a reference.
if [[ -f "$stamp" && "$(cat "$stamp" 2>/dev/null)" == "$ATLAS_CE_SOURCE_SHA256" ]] &&
	atlas_ce_verify_binary "$output" >/dev/null 2>&1; then
	printf 'atlas-ce: reusing the verified binary at %s\n' "$output"
	exit 0
fi
rm -f "$stamp"

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

# The stamp is written LAST, after the binary is in place and verified. A stamp
# written earlier would name a build that had not finished, and the next run
# would reuse whatever the interrupted one left behind.
printf '%s' "$ATLAS_CE_SOURCE_SHA256" >"$stamp"

printf 'atlas-ce: installed verified reference at %s\n' "$output"
atlas_ce_verify_binary "$output"

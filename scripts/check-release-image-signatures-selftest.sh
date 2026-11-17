#!/usr/bin/env bash
# Proves check-release-image-signatures.sh rejects each shape it exists to
# reject.
#
# The gate signs and verifies against two live registries, and only a tagged
# release holds the OIDC identity it signs with, so its rules and its network
# are separate. The rules decide which digest is signed, which identity may
# sign it, and which verified signature counts, and the self-test drives each
# of them through an accepting control and every rejection with no registry and
# no cosign binary: cosign is a function that records its calls, and the
# verified claims are read from a real cosign v3 output kept in
# scripts/testdata (stokaro/ptah#5977).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
exec bash "$repo_root/scripts/check-release-image-signatures.sh" --selftest

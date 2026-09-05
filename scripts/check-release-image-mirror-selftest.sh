#!/usr/bin/env bash
# Proves check-release-image-mirror.sh rejects each shape it exists to reject.
#
# The gate reads two live registries, so its rule and its network are separate:
# `compare_digests` decides, and the self-test drives every branch of it with
# no registry at all. That separation is the reason the rule can be proven on a
# pull request while the gate itself only means something after a release.
#
# Four rejections and a control. The control is what keeps a rule that refused
# everything from satisfying the four (stokaro/ptah#2362).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
exec bash "$repo_root/scripts/check-release-image-mirror.sh" --selftest

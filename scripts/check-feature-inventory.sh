#!/usr/bin/env bash
# Checks docs/feature-inventory.json against the declarations it is derived from.
#
# One gate, because there is one rule: the committed artifact equals the
# regeneration. Every derived value is covered by that single byte comparison.
# The attempt this replaces needed five gates because it had five kinds of
# authored text to police, and each of those gates had to guess whether the
# typing was true -- which is how a canonical-page check came to accept any page
# containing any token of a feature's name (stokaro/ptah#2402).
#
# What the generator refuses on, beyond the byte comparison:
#
#   empty-kind            a row kind derived nothing. A gate that discovers zero
#                         inputs reports the same success as one that checked
#                         everything, which is why every source here has a floor.
#   identifier-collision  two surfaces derived one identifier.
#   unknown-claim         a page's `owns:` names a feature the derivation does
#                         not produce. Reported with the page and the id, never
#                         silently dropped.
#   duplicate-claim       two pages claim one feature. Both named.
#   claimed-below-floor   documentation coverage fell below the floor that
#                         internal/featureinventory.ClaimedFloor holds. The
#                         floor is a source constant, not a number the artifact
#                         carries forward: one read out of the file it guards is
#                         the one field a byte comparison cannot police, so an
#                         edited line would lower it and the gate would report
#                         success.
#   no-examples           no page opts in to internal/quickstart.
#   example-runs-nothing  a page opts in and publishes no step. The marking is
#                         deliberate, but a deliberate marking is still a claim,
#                         and a page of prose carrying it would otherwise be
#                         published under runnable_examples while running
#                         nothing.
#
# `--write` regenerates the artifact. It resolves no rule: every one of them is
# somebody's mistake, and rewriting the file around one records the mistake as
# the new truth.
# `--selftest` breaks each rule above against in-memory fixtures and requires
# the derivation to notice -- including a control fixture that must stay clean,
# because a derivation refusing everything would satisfy every other case while
# gating nothing.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

case "${1-}" in
"") go run ./internal/cmd/featureinventory ;;
--write) go run ./internal/cmd/featureinventory --write ;;
--selftest) go run ./internal/cmd/featureinventory --selftest ;;
*)
	echo "usage: $0 [--write | --selftest]" >&2
	exit 2
	;;
esac

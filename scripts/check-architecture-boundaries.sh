#!/usr/bin/env bash
# Refuses architecture-boundary debt that GREW.
#
# ADR 0001 section 3.2 names four forbidden dependency directions. Two of them
# are violated today and two are already at zero, so this gate is a ratchet
# rather than a wall: the recorded counts in docs/architecture_boundaries.json
# may fall and may never rise (stokaro/ptah#1344).
#
# The measurement is the type checker's, never the source text's. An import edge
# is one the compiler resolved and a construction site is a composite literal
# whose type it confirmed, so a doc comment showing a caller how to build a
# schema is not a finding -- which a search for the type's spelling reports as
# one, twice, as the record in ADR 0001 notes.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

go run ./internal/cmd/boundaries

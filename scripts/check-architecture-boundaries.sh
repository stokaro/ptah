#!/usr/bin/env bash
# Refuses architecture-boundary debt that GREW.
#
# The rules start from the forbidden dependency directions of ADR 0001 section
# 3.2. A rule may land while the tree still violates it, so this gate is a
# ratchet rather than a wall: the recorded counts in
# docs/architecture_boundaries.json may fall and may never rise
# (stokaro/ptah#1344), and a fall must be recorded.
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

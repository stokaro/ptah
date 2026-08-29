#!/usr/bin/env bash
# Checks every generated documentation block against the generator it comes from.
#
# `--write` regenerates them instead.
#
# Five scripts stood here, one per generated surface, and between them they
# carried 650 lines of the same engine: argument parsing, an awk block
# extractor, carrier discovery, diff rendering, and five byte-for-byte copies of
# an embedded Python program that replaces the text between two markers. A sixth
# generated surface invited a sixth copy (stokaro/ptah#2510).
#
# The engine is internal/docsync and the declaration of what to keep in step is
# internal/cmd/docsync. This file exists so the gate is where a contributor
# looks for one, and so scripts/check-gate-selftests.sh can drive it.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
exec go run ./internal/cmd/docsync "$@"

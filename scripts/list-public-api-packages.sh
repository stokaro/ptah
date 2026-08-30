#!/usr/bin/env bash
# Prints the stable embedder package set from docs/public_api.md. The parser
# lives in internal/featureinventory so every public-API check uses one answer.
set -euo pipefail

export GOWORK=off

repo_root="$(git rev-parse --show-toplevel)"
go -C "$repo_root" run ./internal/cmd/featureinventory --list-ledger --root "$repo_root"

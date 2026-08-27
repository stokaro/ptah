#!/usr/bin/env bash
set -euo pipefail

# Renovate evaluates custom-manager patterns with RE2. A pattern RE2 cannot
# compile does not degrade to a missed bump: Renovate refuses the configuration
# and stops opening pull requests entirely, which is what stokaro/ptah#2339 was.
#
# `renovate-config-validator` does not catch it. It loads the `re2` native module
# and falls back to JavaScript's engine when that module is absent, and
# JavaScript has the backreference RE2 lacks -- measured on 2026-08-27, it
# reported success for the exact file Renovate had already refused.

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

go run ./internal/cmd/renovateregex

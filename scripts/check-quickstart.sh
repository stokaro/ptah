#!/usr/bin/env bash
# Runs the commands the quick-start pages publish, and fails if any of them does
# not do what the page says it does.
#
# The commands are read out of the pages rather than kept here. A runner holding
# its own copy of a transcript stays green while the page it claims to cover
# rots, which is the whole failure this check exists to prevent -- so a step
# reworded on the page is the step that runs on the next pull request, and a
# step deleted from the page stops being covered by the same commit that deleted
# it.
#
# A page opts in with `quickstart: true` in its frontmatter. Everything else the
# runner needs it reads from the page's own shape, which is the shape
# docs/STYLE_GUIDE.md section 8 already asks for: commands and output in
# separate blocks, output introduced by a sentence that names the stream.
#
# Discovery fails closed. internal/quickstart carries floors on the number of
# pages, steps and assertions found, because a run that discovered nothing
# reports exactly what a run that checked everything reports.
#
# This wrapper is thin on purpose. The extraction has to drive a Bash block on
# Linux and macOS and a PowerShell block on Windows, so it lives in Go, in
# internal/quickstart, with one implementation instead of two shell runners that
# would drift.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# No arguments of its own: everything after the script name goes to the runner,
# which documents its own flags under `--help`.
exec go run ./internal/cmd/quickstart run "$@"

#!/usr/bin/env bash
# Proves check-architecture-boundaries.sh fails on each shape it forbids.
#
# The gate passes on the tree as it stands, and a gate whose only observed
# result is "pass" is indistinguishable from one that examines nothing.
# stokaro/ptah#1344 requires an inverse control for exactly that reason: an
# invariant that has never been seen fail is not accepted as evidence.
#
# Three defects are introduced in turn, each in a throwaway copy of the tree:
# a NEW forbidden import on a rule already at zero, a NEW one on a rule with
# recorded debt, and a source-description construction the type checker can see.
# A fourth case proves the opposite direction -- that a doc comment showing a
# caller how to build a schema is NOT a finding, which is the false positive a
# spelling-based check produces.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-boundaries-selftest.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

tree="$work_dir/tree"
git -C "$repo_root" worktree list >/dev/null 2>&1
mkdir -p "$tree"
# A copy rather than a worktree: the defects below must never reach a branch.
tar -C "$repo_root" --exclude='.git' -cf - . | tar -C "$tree" -xf -
git -C "$tree" init --quiet
git -C "$tree" add -A >/dev/null 2>&1 || true

run_gate() {
	(cd "$tree" && bash scripts/check-architecture-boundaries.sh >/dev/null 2>&1)
}

require_refusal() {
	local what="$1"
	if run_gate; then
		echo "check-architecture-boundaries-selftest: the gate ACCEPTED $what" >&2
		exit 1
	fi
}

require_acceptance() {
	local what="$1"
	if ! run_gate; then
		echo "check-architecture-boundaries-selftest: the gate REFUSED $what" >&2
		exit 1
	fi
}

require_acceptance "the unmodified tree"

# 1. A new forbidden import on a rule recorded at zero. This is the case the
#    gate must catch outright rather than tolerate against a baseline.
target="$tree/migration/planner/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package planner

import _ "go.5x5.cz/ptah/migration/migrator"
GO
require_refusal "planning importing versioned execution"
rm -f "$target"
require_acceptance "the repaired tree"

# 2. A new forbidden import on a rule that already carries debt. A ratchet that
#    only checked the zero rules would pass this.
target="$tree/core/goschema/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package goschema

import _ "go.5x5.cz/ptah/internal/convert/toschema"
GO
require_refusal "the canonical model taking one more pipeline import"
rm -f "$target"
require_acceptance "the repaired tree"

# 3. A source-description construction inside a planner.
target="$tree/internal/planner/dialects/sqlite/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package sqlite

import "go.5x5.cz/ptah/core/goschema"

func boundariesSelftestDefect() *goschema.Database {
	return &goschema.Database{}
}
GO
require_refusal "a planner constructing a source schema description"
rm -f "$target"
require_acceptance "the repaired tree"

# 4. An IMPROVEMENT must also fail, until it is recorded. A ceiling nobody
#    lowers is not a ratchet: leaving the old number would let the debt return
#    to it with the gate green the whole way.
baseline="$tree/docs/architecture_boundaries.json"
cp "$baseline" "$work_dir/baseline.orig"
python3 -c "import json,sys; p=sys.argv[1]; d=json.load(open(p)); d['rules']['model-imports-pipeline']+=1; json.dump(d,open(p,'w'),indent=2)" "$baseline"
require_refusal "recorded debt higher than the tree's"
cp "$work_dir/baseline.orig" "$baseline"
require_acceptance "the restored baseline"

# 5. The inverse of case 3, and the reason this gate reads types rather than
#    text: the same spelling inside a DOC COMMENT is not a construction, and
#    must not be counted. A search for the type name reports it as one.
target="$tree/internal/planner/dialects/sqlite/boundaries_selftest_comment.go"
cat >"$target" <<'GO'
package sqlite

// boundariesSelftestComment shows a caller how to build a description:
//
//	generated := &goschema.Database{}
//
// which is prose, not a construction site.
func boundariesSelftestComment() {}
GO
require_acceptance "a doc comment that merely names the type"
rm -f "$target"

echo "check-architecture-boundaries-selftest: OK (4 refusals, 1 false-positive control)"

#!/usr/bin/env bash
# Proves check-architecture-boundaries.sh fails on each shape it forbids.
#
# The gate passes on the tree as it stands, and a gate whose only observed
# result is "pass" is indistinguishable from one that examines nothing.
# stokaro/ptah#1344 requires an inverse control for exactly that reason: an
# invariant that has never been seen fail is not accepted as evidence.
#
# Defects are introduced in a throwaway copy of tracked source files. Each
# refusal is followed by a repaired-tree control. A comment that names a
# forbidden construction must remain accepted.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/ptah-boundaries-selftest.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT INT TERM

tree="$work_dir/tree"
git -C "$repo_root" worktree list >/dev/null 2>&1
mkdir -p "$tree"
# A copy rather than a worktree: the defects below must never reach a branch.
git -C "$repo_root" ls-files -z | tar -C "$repo_root" --null -T - -cf - | tar -C "$tree" -xf -
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

import _ "ptah.run/migration/migrator"
GO
require_refusal "planning importing versioned execution"
rm -f "$target"
require_acceptance "the repaired tree"

# 2. A new forbidden import on a rule that already carries debt. A ratchet that
#    only checked the zero rules would pass this.
target="$tree/core/goschema/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package goschema

import _ "ptah.run/internal/sqlschema"
GO
require_refusal "the canonical model taking one more pipeline import"
rm -f "$target"
require_acceptance "the repaired tree"

# 3. A source-description construction inside a planner.
target="$tree/internal/planner/dialects/sqlite/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package sqlite

import "ptah.run/core/goschema"

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

# Provider isolation is transitive. A compilable helper must not smuggle a
# concrete feature into the public contract's dependency graph.
mkdir -p "$tree/feature/boundaryfixture" "$tree/internal/boundaryfixture"
cat >"$tree/feature/boundaryfixture/feature.go" <<'GO'
package boundaryfixture
GO
cat >"$tree/internal/boundaryfixture/bridge.go" <<'GO'
package boundaryfixture

import _ "ptah.run/feature/boundaryfixture"
GO
cat >"$tree/engine/boundaries_selftest_defect.go" <<'GO'
package engine

import _ "ptah.run/internal/boundaryfixture"
GO
(cd "$tree" && go build ./engine)
require_refusal "a provider contract transitively linking a concrete feature"
rm -f "$tree/engine/boundaries_selftest_defect.go"
require_acceptance "the repaired provider contract"

# Captured parents are contracts too. They must remain usable by an external
# provider without importing concrete features through common model helpers.
cat >"$tree/core/schemacapture/boundaries_selftest_defect.go" <<'GO'
package schemacapture

import _ "ptah.run/internal/boundaryfixture"
GO
(cd "$tree" && go build ./core/schemacapture)
require_refusal "captured parent contracts transitively linking a concrete feature"
rm -f "$tree/core/schemacapture/boundaries_selftest_defect.go"
require_acceptance "the repaired parent contract"

# Contextual feature planning must remain independent from target owners too.
cat >"$tree/core/featureplan/boundaries_selftest_defect.go" <<'GO'
package featureplan

import _ "ptah.run/internal/boundaryfixture"
GO
(cd "$tree" && go build ./core/featureplan)
require_refusal "feature planning contracts transitively linking a concrete feature"
rm -f "$tree/core/featureplan/boundaries_selftest_defect.go"
require_acceptance "the repaired feature planning contract"

# Validation is a provider contract, independent from concrete owners.
cat >"$tree/core/schemavalidation/boundaries_selftest_defect.go" <<'GO'
package schemavalidation

import _ "ptah.run/internal/boundaryfixture"
GO
(cd "$tree" && go build ./core/schemavalidation)
require_refusal "schema validation contracts transitively linking a concrete feature"
rm -f "$tree/core/schemavalidation/boundaries_selftest_defect.go"
require_acceptance "the repaired validation contract"

# A consumer must use its caller's renderer, including through helper packages.
mkdir -p "$tree/internal/renderboundaryfixture" "$tree/engine/builtinlookalike"
cat >"$tree/internal/renderboundaryfixture/bridge.go" <<'GO'
package renderboundaryfixture

import _ "ptah.run/engine/builtin"
GO
cat >"$tree/internal/genexprprobe/boundaries_selftest_defect.go" <<'GO'
package genexprprobe

import _ "ptah.run/internal/renderboundaryfixture"
GO
(cd "$tree" && go build ./internal/genexprprobe)
require_refusal "a rendering consumer transitively linking a built-in factory"

# A similar package name is not the forbidden package or one of its children.
cat >"$tree/engine/builtinlookalike/empty.go" <<'GO'
package builtinlookalike
GO
cat >"$tree/internal/renderboundaryfixture/bridge.go" <<'GO'
package renderboundaryfixture

import _ "ptah.run/engine/builtinlookalike"
GO
require_acceptance "a consumer dependency with a similar package name"
rm -f "$tree/internal/genexprprobe/boundaries_selftest_defect.go"
require_acceptance "the repaired rendering consumer"

# The neutral layer links no concrete owner, through any chain of imports.
# core/astbuilder is clean and outside every provider contract's graph, so this
# rule alone can refuse it.
cat >"$tree/core/astbuilder/boundaries_selftest_defect.go" <<'GO'
package astbuilder

import _ "ptah.run/internal/boundaryfixture"
GO
(cd "$tree" && go build ./core/astbuilder)
require_refusal "a neutral package transitively linking a concrete feature"
rm -f "$tree/core/astbuilder/boundaries_selftest_defect.go"
require_acceptance "the repaired neutral package"

# A neutral package that already carries owner debt must not take another
# owner. core/goschema links several dialects today; a ratchet counting
# packages rather than owners would pass this.
cat >"$tree/core/goschema/boundaries_selftest_defect.go" <<'GO'
package goschema

import _ "ptah.run/internal/boundaryfixture"
GO
(cd "$tree" && go build ./core/goschema)
require_refusal "a source frontend with owner debt linking one more owner"
rm -f "$tree/core/goschema/boundaries_selftest_defect.go"
require_acceptance "the repaired source frontend"

echo "check-architecture-boundaries-selftest: OK (11 refusals, 2 false-positive controls)"

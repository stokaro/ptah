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

# A source frontend in the neutral layer must not link an owner either.
# core/goschema carried owner debt until stokaro/ptah#4140 took the rule to
# zero, so no package carries any now; the case stays as a refusal in a second
# import graph, apart from core/astbuilder's.
cat >"$tree/core/goschema/boundaries_selftest_defect.go" <<'GO'
package goschema

import _ "ptah.run/internal/boundaryfixture"
GO
(cd "$tree" && go build ./core/goschema)
require_refusal "a source frontend linking an owner"
rm -f "$tree/core/goschema/boundaries_selftest_defect.go"
require_acceptance "the repaired source frontend"

# Go annotation export consumes selected encoders. Concrete owner imports in
# its tests are valid fixtures; production imports must fail the owner rule.
target="$tree/internal/convert/goschematogo/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package goschematogo

import _ "ptah.run/feature/boundaryfixture"
GO
(cd "$tree" && go build ./internal/convert/goschematogo)
require_refusal "Go annotation export directly linking a concrete feature"
rm -f "$target"
require_acceptance "the repaired Go annotation writer"

# Moving the owner import behind an internal helper must not evade the rule.
cat >"$target" <<'GO'
package goschematogo

import _ "ptah.run/internal/boundaryfixture"
GO
(cd "$tree" && go build ./internal/convert/goschematogo)
require_refusal "Go annotation export transitively linking a concrete feature"
rm -f "$target"
require_acceptance "the repaired Go annotation writer's import graph"

# Cleanup checks selected owners through the annotation contract too.
target="$tree/internal/goannotationcleanup/boundaries_selftest_defect.go"
for dependency in feature/boundaryfixture internal/boundaryfixture; do
  cat >"$target" <<GO
package goannotationcleanup

import _ "ptah.run/$dependency"
GO
  (cd "$tree" && go build ./internal/goannotationcleanup)
  require_refusal "Go annotation cleanup linking $dependency"
  rm -f "$target"
  require_acceptance "the repaired Go annotation cleanup"
done

# The shared SQL reader links no owner. internal/sqlschema is outside the
# neutral layer and every provider contract, so this rule alone can refuse it.
target="$tree/internal/sqlschema/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package sqlschema

import _ "ptah.run/internal/boundaryfixture"
GO
(cd "$tree" && go build ./internal/sqlschema)
require_refusal "the SQL schema reader transitively linking a concrete feature"
rm -f "$target"
require_acceptance "the repaired SQL schema reader"

target="$tree/internal/parser/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package parser

import _ "ptah.run/feature/boundaryfixture"
GO
(cd "$tree" && go build ./internal/parser)
require_refusal "the shared SQL parser directly linking a concrete feature"

# A dialect helper outside the owner families is allowed: it interprets an
# object of the common model, as internal/pgname names an unnamed constraint.
cat >"$target" <<'GO'
package parser

import _ "ptah.run/internal/pgname"
GO
(cd "$tree" && go build ./internal/parser)
require_acceptance "the shared SQL parser importing a dialect helper"
rm -f "$target"

# The recorded debt must fall with the tree: a baseline above the measured
# count is refused until -update lowers it.
cp "$baseline" "$work_dir/baseline.orig"
python3 -c "import json,sys; p=sys.argv[1]; d=json.load(open(p)); d['rules']['sql-reader-links-owners']+=1; json.dump(d,open(p,'w'),indent=2)" "$baseline"
require_refusal "a SQL reader baseline higher than the tree's"
cp "$work_dir/baseline.orig" "$baseline"
require_acceptance "the restored SQL reader baseline"

# Shared packages must not name a dialect (stokaro/ptah#4204). core/astbuilder
# names none today, so each defect below is a package the baseline permits
# nothing.
target="$tree/core/astbuilder/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package astbuilder

import "ptah.run/core/platform"

var _ = platform.Postgres
GO
require_refusal "a shared package naming a dialect constant"
rm -f "$target"
require_acceptance "the repaired shared package"

# migration/migrator already carries debt. One more reference is a package
# count that grew, which a check of the total alone would also see; the move
# case below is the one only a per-package check sees.
target="$tree/migration/migrator/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package migrator

import "ptah.run/core/platform"

var _ = platform.MySQL
GO
require_refusal "a package with recorded debt naming one more dialect"
rm -f "$target"

# A literal in a switch case is a dialect name without the constant.
target="$tree/core/astbuilder/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package astbuilder

func boundariesSelftestDefect(dialect string) bool {
	switch dialect {
	case "mysql":
		return true
	}
	return false
}
GO
require_refusal "a shared package switching on a dialect literal"

# A family predicate is a dialect branch too; replacing a comparison with one
# must not read as progress.
cat >"$target" <<'GO'
package astbuilder

import "ptah.run/core/platform"

var _ = platform.IsPostgresFamily("x")
GO
require_refusal "a shared package calling a dialect family predicate"
rm -f "$target"

# The spelling outside a comparison, and the constant named in a doc comment,
# are words, not dialect branches.
cat >"$target" <<'GO'
package astbuilder

// boundariesSelftestLabel is not platform.Postgres, and is not compared.
var boundariesSelftestLabel = "postgres"
GO
require_acceptance "a dialect spelling in a comment and in an uncompared string"
rm -f "$target"

# An owner, a per-engine helper and the built-in bundle may name engines. Each
# is excluded from the shared set by a different clause.
for dependency in dialect/postgres/pglint:pglint internal/pgname:pgname internal/builtinlowering:builtinlowering; do
  dir="${dependency%%:*}"
  name="${dependency##*:}"
  target="$tree/$dir/boundaries_selftest_owner.go"
  cat >"$target" <<GO
package $name

import "ptah.run/core/platform"

var _ = platform.YDB
GO
  require_acceptance "$dir naming a dialect"
  rm -f "$target"
done

# A collection of dialect names is a dialect branch wherever it is searched,
# so the names count where the collection is written, and so does the value a
# membership test searches for. Each shape is its own defect.
target="$tree/core/astbuilder/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package astbuilder

import "slices"

func boundariesSelftestDefect(dialect string) bool {
	return slices.Contains([]string{"mysql", "mariadb"}, dialect)
}
GO
require_refusal "a shared package listing dialect names in a slice literal"
cat >"$target" <<'GO'
package astbuilder

import "slices"

func boundariesSelftestDefect(names []string) bool {
	return slices.Contains(names, "sqlite")
}
GO
require_refusal "a shared package searching a slice for a dialect name"
cat >"$target" <<'GO'
package astbuilder

var boundariesSelftestDefect = map[string]int{"postgres": 1}
GO
require_refusal "a shared package keying a map literal by a dialect name"
cat >"$target" <<'GO'
package astbuilder

func boundariesSelftestDefect(byDialect map[string]int) int {
	return byDialect["ydb"]
}
GO
require_refusal "a shared package looking a dialect name up in a map"

# The same shapes over other words, and a dialect spelling in a struct field,
# are not dialect branches.
cat >"$target" <<'GO'
package astbuilder

import "slices"

var boundariesSelftestWords = map[string]int{"alpha": 1}

var boundariesSelftestRole = struct{ Name string }{Name: "postgres"}

func boundariesSelftestControl(byWord map[string]int) bool {
	return slices.Contains([]string{"alpha", "beta"}, "alpha") && byWord["beta"] > 0
}
GO
require_acceptance "collections, a lookup and a struct field holding no dialect name"
rm -f "$target"

# A package outside the in-process YDB reading that imports the YQL parser,
# which moves into the YDB provider.
target="$tree/internal/schemafile/boundaries_selftest_defect.go"
cat >"$target" <<'GO'
package schemafile

import _ "ptah.run/internal/yqlparse"
GO
require_refusal "a new importer of the YQL parser"
rm -f "$target"
require_acceptance "the repaired tree"

# A fall must be recorded package by package. A baseline that still permits
# one more reference in a package is refused, and so is debt that moved
# between two packages with the total unchanged. The packages are the first
# two the baseline still records, so a package that reaches zero and leaves
# the baseline does not break this check. Where the baseline records one
# package, the debt moves to internal/parser, a shared package the rule
# requires and that names no dialect.
cp "$baseline" "$work_dir/baseline.orig"
python3 -c "import json,sys; p=sys.argv[1]; d=json.load(open(p)); r='shared-packages-name-dialects'; k=sorted(n for n,c in d['packages'][r].items() if c>0); d['packages'][r][k[0]]+=1; d['rules'][r]+=1; json.dump(d,open(p,'w'),indent=2)" "$baseline"
require_refusal "a package baseline higher than the package's count"
cp "$work_dir/baseline.orig" "$baseline"
python3 -c "import json,sys; p=sys.argv[1]; d=json.load(open(p)); r='shared-packages-name-dialects'; k=sorted(n for n,c in d['packages'][r].items() if c>0)+['internal/parser']; d['packages'][r][k[1]]=d['packages'][r].get(k[1],0)+1; d['packages'][r][k[0]]-=1; json.dump(d,open(p,'w'),indent=2)" "$baseline"
require_refusal "dialect debt recorded in the wrong package with the total unchanged"
cp "$work_dir/baseline.orig" "$baseline"
require_acceptance "the restored dialect-name baseline"

echo "check-architecture-boundaries-selftest: OK (29 refusals, 8 false-positive controls)"

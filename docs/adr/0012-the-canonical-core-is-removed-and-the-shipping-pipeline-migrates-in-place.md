# ADR 0012: The canonical core is removed, and the shipping pipeline migrates in place

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#2315](https://github.com/stokaro/ptah/issues/2315)
- Supersedes in part: sections 8 and 9 of
  [ADR 0001](0001-canonical-schema-state-and-pipeline-boundaries.md) — the
  staged migration through a parallel canonical core, and the slice selected to
  prove it

## 1. Context

ADR 0001 chose to build one canonical schema state beside the pipeline that
ships, prove it on a vertical slice, and migrate the product onto it in stages.
The state model was built. `internal/schemastate` and `internal/schemachange`
hold 4905 lines of code and 5674 lines of tests across 30 files, and everything
ADR 0001 asked of them is there: one model for both sides, identity off source
metadata, typed changes carrying risk and required facts, one dependency graph.

**They have no non-test importer anywhere in the tree.** `go list -deps
./cmd/...` reaches `internal/objectidentity` and `internal/deporder`, and
neither of these two. The product plans and applies through
`schemamodel.Database`, `difftypes.SchemaDiff` and `migration/planner`, exactly
as it did before ADR 0001 landed.

That is not a failure of the model. It is what a parallel core costs: the
migration stages that would have connected it are each large enough to be
deferred, and deferring them is free — nothing goes red, because nothing depends
on it.

### 1.1 What the parallel core cost while it waited

The measurement that decided this ADR was not a line count. Over one working
session, the **shipping** comparison was found to build its expression-
resolution map keys in five hand-written functions across four object families —
CHECK constraints, RLS policies, generated columns and index expressions — each
of which said in its own doc comment that it had to agree with the others. They
agreed on `strings.ToLower` over the components joined with a dot, which is the
fold of no target Ptah supports and a join
[object identity](../object_identity.md) invariant 5 forbids.

Two of the four were reachable defects that silently swallowed a real
difference:

- PostgreSQL holds `t` and `T` as two tables, and a constraint name is unique
  within its table. One key for both meant the expression a server resolved for
  one table answered the lookup for the other, so a declared CHECK that differed
  from the catalog's was reported as no change and never applied.
- The same shape for index predicates, where PostgreSQL keeps index names in the
  schema namespace and preserves their case.

Both are fixed, in the shipping pipeline, with the fold that
`identifier.Semantics` supplies and the components kept apart. The canonical
core had modeled all of this correctly since #1663 and prevented none of it,
because nothing calls it.

A core that holds the right model and is not on the path does not make the
product correct. It makes two models to keep correct.

## 2. Decision

1. **Stop extending the prototype.** No new families, adapters or invariants are
   added to `internal/schemastate` or `internal/schemachange`.
2. **Tag it, then remove it from master.** The complete prototype is reachable at
   a tag, and its reasoning stays in ADR 0001, in
   [the prototype report](../canonical_pipeline_prototype.md) and in the closed
   issues that built it. Nothing is lost that a reader cannot reach.
3. **Migrate the shipping pipeline in place**, incrementally, toward
   self-contained typed changes: a change that carries what a planner needs
   rather than a name the planner recovers from a second parameter.

## 3. What was kept, and where it already lives

Before removal, every primitive and invariant in the pair was checked for a
production consumer. **None of them is the sole holder of one.**

| Invariant | Held in production by |
| --- | --- |
| Identity and references: distinct objects never collapse; dangling, ambiguous and normalized-collision references are refused | `internal/objectidentity` — a separate package, reached from `cmd/` |
| Dependency ordering, including view-like objects that read each other | `internal/deporder` — separate, used by the shipping planner |
| Coverage: not-inspected never becomes absent | `core/coverage` — separate, used by the shipping comparison |
| Routine overload identity | `recordedRoutineSignature` and the signature-carrying removal lists in the shipping comparator |
| A composite key is a key, for a foreign key's referenced columns | `renderer.tableHasUniqueKey`, which accepts a primary key, a unique field, a unique constraint or a unique index, each compared as a full column list |

The last two rows are the ones
[architecture boundaries](../architecture_boundaries.md) recorded as closed and
credited to `schemastate`. The boundary is closed; the credit was misplaced, and
that document is corrected rather than left to be falsified by this removal.

## 4. Consequences

**What gets better.** One model to keep correct instead of two. The defects
above were found by reading the shipping path; the effort that went into keeping
a second model in step is available for that instead.

**What gets worse.** The next attempt at a canonical model starts from a tag and
an ADR rather than from compiling code. That cost is real and it is accepted:
the alternative was to carry 10,579 lines that no user reaches, against a
migration whose every stage was large enough to defer.

**What does not change.** ADR 0001's identity model, its invariant set and its
information-loss inventory are not reopened. Sections 1 through 7 describe the
problem, and the problem is unchanged: there are still two schema states, the
planner still takes the desired description as a second parameter, and
`difftypes.SchemaDiff` still carries 69 parallel per-family fields. This ADR
changes the route, not the destination.

## 5. What replaces the staged plan

#2315 is rewritten as an incremental migration of the pipeline that ships. Each
step has to be shippable on its own and has to leave the product working, which
is the property the parallel core did not have:

1. A change carries its own operands, so `GenerateSchemaDiffAST` stops taking the
   desired description as a second parameter to recover what a name lost.
2. The per-family name lists give way to typed changes, one family at a time,
   measured by the cost of adding a new object family — the most recent genuinely
   new kind cost 27 files across 12 packages.
3. The `internal/convert` packages retire as the families they move stop having
   two spellings.

Nothing here requires a second model to exist first, and each step is visible to
a user the day it lands.

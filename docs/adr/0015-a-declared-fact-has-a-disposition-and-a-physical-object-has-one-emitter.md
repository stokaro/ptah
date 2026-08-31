# ADR 0015: A declared fact carries a disposition, and a physical object has one emitter

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#2606](https://github.com/stokaro/ptah/issues/2606)
- Builds on:
  [ADR 0012](0012-the-canonical-core-is-removed-and-the-shipping-pipeline-migrates-in-place.md)
  — the pipeline that ships is the one that gets corrected, and no second model
  is built beside it

## 1. Context

Between 2026-08-29 and 2026-08-30 five repairs landed, in four packages, for one
shape:

| issue | landed as | where the repair sits |
| --- | --- | --- |
| #2583 | #2601 | `internal/deporder` stopped deriving a self-reference the constraint path already carried |
| #2586 | #2598 | the ClickHouse renderer grew a refusal for one constraint kind |
| #2589 | #2597 | `internal/convert/dbschematogo` learned to prefer the index view for one attribute |
| #2590 item 1 | #2605 | `internal/convert/fromschema` learned to copy `Table.Checks` |
| #2590 item 3 | #2604 | `internal/convert/fromschema` learned to copy `Field.NotNullConstraintName` |

Each is correct. None of them is a fix for the class: a converter walks the
model field by field, and a field nobody taught it about is dropped without a
word. The next field added to `core/schemamodel` has the same odds, and the
render exits 0 either way.

### 1.1 How large the surface is

347 fields are reachable from `schemamodel.Database`, counted by walking the
type graph rather than the file. 323 of them are declared in `schemamodel`; the
rest are reached through it, and a CockroachDB row-TTL parameter that lives in
`core/ast` is exactly as droppable as one that does not.

### 1.2 One question, twelve answers

The second half of #2606 is ownership, and it was measured the same way. Whether
an index is the physical backing of a constraint — and which of the two a change
should be spelled through — is decided in **twelve** production functions across
five package trees:

```text
internal/convert/dbschematogo   constraintBackedIndexesByTable
internal/planner/dialects/postgres  constraintBackedIndexDropNode
internal/planner/dialects/mysql     primaryKeyColumnChangeOwnedByTableConstraint
migration/generator                 addMySQLFamilyForeignKeyBackingIndexRemovals
migration/generator                 addGeneratedBackingIndex
migration/schemadiff/.../compare    isConstraintBasedUniqueIndex
migration/schemadiff/.../compare    isMySQLConstraintBasedUniqueIndex
migration/schemadiff/.../compare    constraintBackedIndexIdentities
migration/schemadiff/.../compare    spannerForeignKeyBackingIndexes
migration/schemadiff/.../compare    constraintOwnedDatabaseIndex
migration/schemadiff/.../compare    isSQLiteInternalAutoindex
migration/schemadiff/.../compare    uniqueConstraintOwnedByDeclaredIndex
```

Two of them answer from the index's **name**. `isConstraintBasedUniqueIndex`
reads `users_email_key` as constraint-backed because it ends in `_key` and
begins with the table's name, and `constraintOwnedDatabaseIndex` reaches that
heuristic whenever the catalog-derived sets do not answer. A plain unique index
a person named that way is not a different object because of its spelling.

### 1.3 The host is named twice, and the second name is optional

A table-owned object carries `StructName`, the Go struct it was read from, and
`Table`/`TableName`, the database name. Measured on `postgres`: either alone
resolves the host, both together resolve to `Table` with no complaint, and
neither resolves to whichever behavior the family happens to have — a CHECK is
dropped, an index renders `ON ""`, an RLS enable renders
`ALTER TABLE "" ENABLE ROW LEVEL SECURITY`. All three at exit 0
([#2612](https://github.com/stokaro/ptah/issues/2612)).

## 2. Definitions

These are the words the rest of this record and #2606 use. They are written down
because the twelve functions above each named the same idea differently.

**Semantic object** — what the author declared: a primary key, a unique
constraint, an exclusion constraint, a foreign key. It has one identity.

**Host relation** — the table or view the semantic object belongs to. Resolved
once, from whichever spellings a declaration uses, and refused rather than
guessed when a declaration names none or names two.

**Physical backing** — the object the target actually creates to enforce the
semantic one: a unique index behind a UNIQUE constraint, an index-like object
behind an EXCLUDE. It is dialect-specific and it is not decided by a name match.

**Emission owner** — the one of those two that a change is spelled through.
Exactly one, per semantic object, per direction. A backing object may carry
attributes the semantic object does not — an access method, an INCLUDE payload,
a predicate, operator classes, storage parameters — and the owner consults them
rather than discarding them.

**Disposition** — what a declared field is for, from a closed set. Seven values,
enumerated in `internal/schemacensus`: rendered, comparison-only,
planning-context, derived, source provenance, export metadata, and reference
data. There is no eighth value meaning "populated, and nobody reads it".

**Ambiguity** — two candidate hosts, two candidate backings, or a declaration
that resolves to neither. It is refused with both candidates named, and it is
never resolved by position, by count, or by which one the code happened to check
first.

## 3. Decision

### D1. Every reachable field carries a disposition, and the DDL ones are measured

`internal/schemacensus` enumerates the fields by reflection, so a new one is a
decision rather than an omission; and it verifies the `ddl` ones by ablation —
remove the field from a fixture, render again on every declared release line,
and require the output to move.

The registry is the only hand-written half, and nothing trusts it: the field
list decides which entries must exist, and the measurement decides whether a
`ddl` entry is telling the truth. A field that should render and does not is
recorded as a gap naming its issue, and the gate fails **in both directions** —
a gap the census can see is a repair that must be reclassified in the same
change.

### D2. Ownership is resolved once, per dialect, from evidence

One resolver answers "is this index the backing of that constraint, and which is
the emitter". Every path that needs the answer consults it: the catalog
converter, the comparator's two pools, the planners, and the reverse direction.

The rule is neither "constraint always wins" nor "index always wins". It is
per-target and measured, and where two catalog records cannot be joined
unambiguously it fails closed and names both candidates. A name match is
evidence about a name; it is not evidence about an object.

### D3. A host is resolved before anything else looks at the object

Fields, constraints, indexes, triggers and policies are attached to a resolved
host once, and no later stage re-derives one. A declaration that names no host
is refused; one that names two different hosts is refused with both.

### D4. This is a compiled projection, not a second core

The preparation runs on the shipping path, is consumed by the same workflow that
builds it, and holds no state the model does not. It gains no feature the
product cannot reach through `ptah`.

ADR 0012 recorded what the alternative costs: `internal/schemastate` and
`internal/schemachange` held 10,579 lines that modeled all of this correctly and
prevented none of the defects, because nothing called them. **A model that is
not on the path does not make the product correct; it makes two models to keep
correct.** The test that a projection has not become a core is that removing it
breaks the product, which is not true of anything a parallel model holds.

## 4. Alternatives

**A registry with no measurement.** One line per field saying what it is for,
reviewed by reading. Rejected: the five repairs above were all in code that
looked right, and a claim about a field is exactly the artifact that goes stale
without being wrong on the day it is written. Measured against this tree, a
review-only register would have recorded six fields as rendered that are not.

**Rendering every field into the SQL and refusing what a target cannot carry.**
Rejected because it is wrong for most of the surface: a Go struct name, an
OpenAPI field name and a coverage record are not database facts, and a rule that
demands they reach SQL produces a refusal for every schema. The closed
disposition set exists so that "this does not render" can be a decision instead
of an accident.

**One universal schema struct.** Rejected by #2606 as a non-goal and by ADR 0001
section 1.1 before it: what a server reports and what a document declares differ
for reasons no refactor removes, and the comparison exists because they are not
the same shape.

## 5. Consequences

**What gets better.** A field added to the model fails a gate until somebody
says what it is for. A field that stops rendering fails a gate the day it stops.
The instrument found five such fields on its first complete run, and forced a
sixth question whose answer was a disposition rather than a repair --
`Index.Concurrently` asks for a non-locking BUILD, which is a fact about adding
an index to a live table and not about the schema. It also found a hang: an
unnamed column made the Oracle renderer scan forever without stopping for
SIGTERM ([#2608](https://github.com/stokaro/ptah/issues/2608)), and an unnamed
host made three families answer three different ways
([#2612](https://github.com/stokaro/ptah/issues/2612)).

**What it costs.** The fixture corpus is real code that has to grow with the
model: a field no fixture declares cannot be measured, and the gate says so
rather than passing. That is the cost of the instrument being honest, and it is
paid once per field rather than once per defect.

**What it cannot do.** It measures that a field is read. It does not measure
that what it produces is correct, and it watches the render path rather than the
comparison and planning paths. Both limits are stated where the gate is
declared, because a check whose reach is not written down is read as covering
everything.

**What does not change.** `schemamodel.Database` and `catalog.Database` stay
separate. `difftypes.SchemaDiff` stays self-contained. No new stable package
appears; `internal/schemacensus` is internal, and the register it generates is
documentation rather than API.

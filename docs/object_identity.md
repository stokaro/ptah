# Object identity and references

This page is the canonical statement of what makes two schema objects the same
object in Ptah, and of how a reference to an object is resolved or refused. The
model lives in `internal/objectidentity`; this page owns the invariants, and the
package's tests are where each one is pinned.

Read it before adding a map keyed on a schema object, before adding a new object
family, and before changing how any existing key folds.

## Why the model exists

Every object family used to grow its own key, and four closed defects came from
exactly that:

| Issue | The key it used | What the key lost |
| --- | --- | --- |
| [#1283](https://github.com/stokaro/ptah/issues/1283) | Grants joined into one delimited string. | Two distinct grants collapsed into one. |
| [#1276](https://github.com/stokaro/ptah/issues/1276) | Policies keyed by policy name alone. | One policy name on two tables collapsed. |
| [#1311](https://github.com/stokaro/ptah/issues/1311) | Policies keyed by `table + "." + policy`. | A component legitimately containing a dot collided with a different pair. |
| [#1302](https://github.com/stokaro/ptah/issues/1302) | Domain columns compared without the schema. | An existing domain read as absent and a destructive drop was planned. |

Each fix was correct for its family and left the others alone, which is how the
same class of defect kept reappearing somewhere else.

## The invariants

### 1. An identity carries every fact that distinguishes the object

An `ID` carries the object kind, catalog, schema, owning parent, own name, and —
for routines — the overload signature. A family that does not use a component
leaves it empty; it never reuses a component to mean something else.

Two objects that differ in any of those facts are two objects.

### 2. The source spelling and the comparison identity are distinct values

Every component is a `Part` holding both what the author or catalog wrote
(`Source`) and what equality is decided on (`Normalized`). Neither is derivable
from the other after the fact:

- a diagnostic quotes `Source`, because a message about the author's schema must
  not contain this model's normalization;
- a renderer emits `Source`, because emitting the folded form puts Ptah's casing
  into the target's DDL;
- a lookup uses `Normalized`, because two spellings of one table are one table.

`ID.Key()` returns the comparison value alone. A map keys on `Key`, never on
`ID` — an `ID` in a map key would make two spellings of one object two entries.

### 3. The encoding is a struct, never a joined string

`Key` is a struct with one field per component. This is not a style preference.
Every component is an SQL identifier, quoting lets any of them contain any
separator, and a joined encoding therefore stops being injective: table
`orders.2024` with policy `p` and table `orders` with policy `2024.p` render as
one string under any separator, and one of two distinct policies is dropped.
That is [#1311](https://github.com/stokaro/ptah/issues/1311) exactly.

A struct key cannot have a component boundary forged by a component's content,
and a component the source did not supply is the zero value of its own field
rather than an absent substring.

### 4. Folding is the target's rule, applied once, to the spelling as written

Normalization runs through `identifier.Semantics` for the dialect. No family
hard-codes a fold.

Folding applies to the spelling **as written**, quotes included. On PostgreSQL
`"Users"` and `Users` are two tables, and a key that stripped the quotes before
folding made them one. The quotes are part of what the author said.

Applying the rule twice is as wrong as not applying it. Where a value has
already been folded by whatever produced it — the planner receives a
`types.SchemaDiff` the comparator already normalized — the verbatim constructors
exist so the second fold does not happen.

### 5. Components held separately are never rejoined

A caller holding a schema and a name as two values uses the `…Parts`
constructors. Joining them with a dot and re-splitting loses the fact that made
them two values: an unqualified table named `tenant.data` comes back as schema
`tenant`, name `data`, which is a different object.

The one-string constructors exist for callers that genuinely have one string,
and their split is quote-aware. The split is not the lossy step; the rejoin is.

### 6. Identity is injective for the domain it models

Two distinct objects never share a `Key`, and one object never has two. `Set`
reports a second `Add` under one key as a collision rather than overwriting, so
a caller validating a schema can detect the case where the target cannot hold
both objects.

### 7. A reference that does not name exactly one object is refused, by class

`Resolve` answers with one of five errors, and they are separate because they
ask the author for different things:

| Error | Meaning | What it asks for |
| --- | --- | --- |
| `ErrAmbiguousReference` | Matches more than one object. | Qualify the reference. |
| `ErrDanglingReference` | Matches no object. | Create the object or correct the name. |
| `ErrNormalizedCollision` | Two candidates fold together and spell differently. | Rename one of the two objects. |
| `ErrInvalidScope` | Crosses a boundary the model does not permit. | Move the reference. |
| `ErrMissingComponent` | The source did not supply a component resolution needs. | Supply the component. |

Collapsing them into one "cannot resolve" would tell an operator that something
is wrong and not what.

### 8. Missing provenance fails closed

Where the source representation cannot preserve a fact safe resolution needs,
resolution refuses rather than picking a nearby object. A policy reference
without its owning table names nothing in particular; resolving it to the first
match would target another object, and the plan would then act on that one.

## Consumers

| Layer | Where | State |
| --- | --- | --- |
| Comparison | `migration/schemadiff/internal/compare` | Migrated. Tables, columns, table members, grants, enums, sequences, domains, composite and range types, views, materialized views, and functions each key through the shared model under their own kind. |
| Diagnostics | `internal/atlasfilter` scope validation | Migrated. Lookups use `Key`, messages quote `Source`. |
| Filtering | `internal/atlasfilter` exclusion state | Migrated, under exact semantics — see below. |
| Planning | `internal/planner/dialects/postgres`, `.../mysql` | Migrated, under verbatim semantics — see below. |
| Rendering | `core/renderer` routine-collision validation | Migrated. It emits `Source` spellings elsewhere and now shares the key it once rebuilt privately. |
| Dependency ordering | `internal/schemafile`, `migration/generator` | Not migrated. |
| Parse-time deduplication | `core/goschema` | Not migrated. |
| Catalog readers | `internal/dbschema/*` | Not migrated, and not a target: those keys index rows of a result set rather than schema objects. |

Everything listed as not migrated keeps a private key that is correct for its
own use today. The remaining work is tracked in
[#1344](https://github.com/stokaro/ptah/issues/1344).

### Routine overload identity

Comparison used to key a routine by schema and name alone, so two overloads of
one name were one object: the second overwrote the first in the comparator's
map. Measured, a dropped overload was reported as a MODIFICATION of the
survivor rather than as a removal, and a new overload was never created — both
answers wrong, both at exit 0 ([#1664](https://github.com/stokaro/ptah/issues/1664)).

The signature is now consulted, and only where a name carries more than one
routine on either side. That restriction is the safety argument rather than an
optimization: a name with one routine on each side pairs exactly as it always
did, so the common case cannot regress on a signature the normalizer spells
differently from the catalog, and the case that does consult it is the one that
was already broken.

The two sides describe arguments differently — a schema declares `a int4`, the
catalog reports `a integer` — so both are reduced by one normalizer whose job is
to AGREE with `pg_get_function_identity_arguments` rather than to reproduce it.
Its regression set is eleven declaration-and-identity pairs measured against
PostgreSQL 18, including the one that corrected an assumption: the catalog keeps
parameter names and drops the redundant `IN` mode.

### Why two consumers fold nothing

`internal/atlasfilter` compares exactly on purpose. An exclude pattern and the
schema it is matched against are written by the same author in the same file, so
the spelling in one is the spelling in the other. Folding there would silently
widen a pattern past what it says: `--exclude Orders` would take `orders` with
it on a target where the two are distinct tables, and the object would leave the
schema without ever being named.

The planners are verbatim for invariant 4: both spellings a constraint host key
is built from arrive from one `types.SchemaDiff`, already normalized by the
comparator that produced it.

## Adapters and their deletion criteria

Two kinds of temporary construct exist. Each has a condition that retires it;
neither is permanent.

**Package-local type aliases.** `compare`, `atlasfilter`, and the planners each
alias `objectidentity.Key` under the name their call sites already used
(`tableIdentity`, `columnIdentity`, `constraintHostKey`). They exist so the
migration is a change of value and not a rename of several hundred lines at
once.

*Delete when* the package's call sites read naturally against the shared
vocabulary — that is, when a reader of `compare` would not be surprised to see
`objectidentity.Key` in a signature. Deleting an alias is a mechanical rename
with no behavior change, and the equivalence tests in
`internal/objectidentity/equivalence_test.go` are what make that safe to assert.

**Verbatim constructors.** `TablePartsVerbatim` and `ConstraintPartsVerbatim`
build identities without folding or trimming.

`TablePartsVerbatim` is not temporary: quoted leading and trailing whitespace is
part of a SQLite table name, so trimming there merges two distinct tables. It
stays as long as SQLite does.

`ConstraintPartsVerbatim` **is** temporary. It exists because
`types.SchemaDiff` carries constraint *spellings* rather than identities, so the
planner cannot fold without folding a second time.

*Delete when* `types.SchemaDiff` carries `objectidentity.ID` values for the
objects it names. The planner then reads identities the comparator already
built, no second fold is possible, and the constructor has no caller.

## Adding a family

1. Add a `Kind` for it. Do not reuse another family's kind because the two
   happen to live in separate maps — a family that later shares a map with
   another then merges silently.
2. Add a constructor on `Builder`, taking components rather than one joined
   string wherever the caller has them separately.
3. Add the collision fixture: the pair of objects that a key without your new
   component would collapse.
4. Add the equality control: the pair that must collapse. A model that answers
   "different" to everything satisfies every collision fixture and is useless.

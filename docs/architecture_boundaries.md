# Architecture boundaries and executable invariants

This page is the human half of the boundary inventory
[#1344](https://github.com/stokaro/ptah/issues/1344) asks for. The machine half
is [`architecture_boundaries.json`](architecture_boundaries.json), and the gate
that reads it is `scripts/check-architecture-boundaries.sh`.

It records what the architecture program in
[ADR 0001](adr/0001-canonical-schema-state-and-pipeline-boundaries.md) is
measured against, so the debt it plans to remove cannot quietly grow while it is
being removed.

## The rule the measurement obeys

Every number here comes from the type checker or the import graph, never from
searching the source text. #1344 states the rule and the reason: a check that
greps for a spelling is bypassable and produces false positives.

That is not hypothetical. Counting one of these rules by searching for a type
name answered wrongly twice — first five files, then eight sites — because a doc
comment showing a caller how to build a schema looks exactly like a caller
building one. The true figure is four. The gate's own self-test carries that
case as a control: a doc comment naming the type must **not** be a finding.

## The four boundaries

ADR 0001 section 3.2 forbids four dependency directions. Two hold today and two
do not, which is why the gate is a ratchet rather than a wall.

| Rule | Property | Recorded |
| --- | --- | --- |
| `model-imports-pipeline` | The canonical model (`core/`) must not import comparison, planning or conversion. | 3 |
| `pipeline-builds-source-description` | A planner or comparator must not construct a source schema description. | 4 |
| `pipeline-imports-execution` | Planning must not import versioned execution. | 0 |
| `renderer-imports-comparator` | A renderer must not import a comparator. | 0 |

A count may fall and may never rise. A rule at zero is therefore enforced
outright: the first violation fails the build.

A fall fails too, until it is recorded with
`go run ./internal/cmd/boundaries -update`. That reads odd until the alternative
is written down: a ceiling nobody lowers is not a ratchet, and leaving the old
number would let the debt return to it with the gate green the whole way.

### What the recorded debt is

The three `model-imports-pipeline` edges:

- `core/renderer` → `internal/convert/fromschema`
- `core/renderer` → `internal/planner/tablelookup`
- `core/schemasource` → `internal/convert/toschema`

The four `pipeline-builds-source-description` sites are one per dialect planner:
`internal/planner/dialects/{clickhouse,mysql,postgres,sqlite}` each build a
`goschema.Database`.

Both are cleared by the staged plan in ADR 0001 section 8, not by this issue.
The gate exists so that work is measurable while it happens.

## Information-loss boundaries

The conversions where a fact can be dropped with nothing to notice, and the
issue that owns each.

| Boundary | What is lost | Owner |
| --- | --- | --- |
| `goschema.Database` ↔ `types.DBSchema` | Two families are spelled differently and several exist on only one side; four packages under `internal/convert` move between them. | [#1662](https://github.com/stokaro/ptah/issues/1662) |
| `types.SchemaDiff` per-family name lists | A change carries a name, so the planner takes the desired description as a second parameter to recover the rest. | [#1662](https://github.com/stokaro/ptah/issues/1662) |
| Converted foreign migration layouts | The rebuilt directory carries no integrity file, so source checksums are dropped. Carried out of band since [#1209](https://github.com/stokaro/ptah/issues/1209). | closed |
| Routine overload identity | Closed: comparison pairs overloads on a signature normalized to agree with the catalog, consulted only where a name is overloaded. | closed |
| Single-column uniqueness | `schemastate.Column.Unique` records single-column keys only, so a composite key reads as not unique. | [#1663](https://github.com/stokaro/ptah/issues/1663) |

## The invariant set

#1344 names seven properties. Each is held by a named test, and each of those
has an inverse control or a recorded mutation run — an invariant nobody has seen
fail is not evidence.

| Property | Held by | Evidence |
| --- | --- | --- |
| Identity: distinct objects never collapse under adversarial names | `internal/objectidentity` defect fixtures | 12 mutants killed, 0 survived ([#1345](https://github.com/stokaro/ptah/issues/1345)) |
| Identifier provenance: quoted and unquoted components round-trip; insufficient provenance fails closed | `objectidentity.Part`, `Builder` equivalence tests | same sweep; folding is asserted equal to `identifier.Semantics` |
| References: dangling, ambiguous and normalized-collision references are rejected | `objectidentity.Resolve` refusal classes | same sweep |
| Coverage: not-inspected never becomes absent | `schemastate.RequireScope` | 15 mutants killed, 0 survived ([#1350](https://github.com/stokaro/ptah/issues/1350)) |
| Target facts: uncertainty reaches every target-dependent consumer | `schemachange` required facts and blocked changes | same sweep |
| Determinism: equivalent inputs produce identical output across runs and map orders | `schemachange` determinism tests | same sweep |
| Package boundaries: compatibility-only packages are not dependencies of the semantic core | `scripts/check-architecture-boundaries.sh` | `…-selftest.sh`: 4 refusals and 1 false-positive control |

## Extending the set

Add a rule when a boundary becomes worth defending, not when it becomes easy to
check.

1. Express it over resolved packages or types in `internal/cmd/boundaries`, so
   it cannot be satisfied by renaming and cannot fire on a name that merely
   resembles a layer.
2. Run `go run ./internal/cmd/boundaries -update` to record what the tree owes
   today. A rule that lands at zero is enforced outright; one that does not
   becomes a ratchet.
3. Add both cases to `scripts/check-architecture-boundaries-selftest.sh`: a
   defect the gate must refuse, and — where the rule could plausibly misfire —
   the shape it must **not** refuse. The existing false-positive control is a
   doc comment naming the type, which is what a spelling-based check gets
   wrong.
4. Name the property and its evidence in the invariant table above.

A gate without step 3 is not accepted. That is the rule #1344 sets and the
reason this file can state its own numbers.

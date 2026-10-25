# ADR 0034: A provider renders text units where the host writes text

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0022](0022-provider-values-are-resolved-in-semantic-batches.md),
[ADR 0024](0024-the-provider-owns-table-changes-through-one-lifecycle-service.md),
[ADR 0028](0028-a-provider-owns-how-rows-are-read-written-and-seeded.md)

Contract: 25.3, with ADRs 0033, 0035, 0036 and 0037.

## Context

A provider renders a change as a unit, and a unit is SQL text or a native
envelope (`Unit.body`). ADR 0024 has the YDB provider alter a table through the
native `AlterTable` call followed by a read-back, so a plan for a provider engine
holds native units. ADR 0028 decided that migration files take text units only,
and that a provider that writes rows through a native call must also render text
for migration use. The contract gives the host no way to ask for that text.

What the code does today:

- `renderer.Result.SQLFragments` (`core/renderer/service.go`) refuses a whole
  result that holds a native fragment with `renderer.ErrNativeSQL`, and returns
  no prefix. Every consumer that writes text goes through it:
  - migration files, in `migration/generator/migrationspec.go`
    (`renderGeneratedMigrationSQL`, `renderGeneratedDownMigrationSQL`);
  - `ptah schema render`, in `internal/cli/generate`, through
    `renderer.RenderSchema`, which `internal/providerhost/schema.go` serves only
    for an engine with `schema.common-table-identity.v1`;
  - `ptah schema diff --format sql`, in `internal/cli/schema/diff.go`, and the
    statements `ptah schema plan` prints (`printPlan` in
    `internal/cli/schema/plan.go`), which list a native unit by its summary;
  - the compatibility verbs `migrate diff` and `schema diff`, in
    `internal/cli/atlas/migrate_diff.go` and `internal/cli/atlas/schema_diff.go`;
  - `ptah schema inspect --format sql`, which writes the inspected schema
    through `renderer.RenderSchema` too (`MarshalSQL` in
    `internal/atlasreport/schema_inspect.go`).
- Row writes take the same path. `internal/providerhost/data.go` refuses a
  `RenderRowWrites` reply with a native unit, because rows are written to
  migration files and checkpoints as statement text
  (`internal/cli/migratedata`, `migration/generator/checkpoint_data.go`).
- `RenderResult.refusal` exists for `RenderRowWrites` only. `Render` has no typed
  way to say that a change has no text form; it can only fail the call.

So a YDB plan that alters a table through the native call cannot be written to a
migration file, and the host has no request that would get the YQL instead.

## Decision

### The host names the form it needs

`RenderRequest` and `RenderRowWritesRequest` gain the form of the units the host
needs:

```proto
// RenderRequest gains:
enum Form {
  ANY = 0;   // SQL text or native units, as today
  TEXT = 1;  // SQL text only
}
Form form = 5;
// RenderRowWritesRequest gains: RenderRequest.Form form = 5;
// RenderRefusal.Reason gains: NO_TEXT_FORM = 3;
```

`form` is a request field the provider has to honor, so it travels only when
both sides list the two-way feature `render.text-units`. The provider lists it in
`Description.features` and in each engine whose changes it can write as text.
Without the feature the host sends no form, and a native unit in a text consumer
is refused with `renderer.ErrNativeSQL`, as today.

### What a text unit promises

In the `TEXT` form every unit is `Unit.sql`. A text unit is portable: its bytes
alone, read back from a file by a later run, make the same provider's `Execute`
do what the native form would do. It carries no envelope, no session state and
no reference to the run that rendered it. The owner decides what text its
executor accepts. It is usually the engine's own statement, such as YQL
`ALTER TABLE`, and it may be a statement only the provider's executor reads,
such as the comment statement YDB's connection turns into a user attribute.
Either way the provider's `AnalyzeSQL` and lint split read it as one statement.

A unit in the `TEXT` form is one execution unit, as every rendered unit is: the
host keeps its bytes and its boundary and never splits it with a host grammar.
Rendering stays deterministic, so a migration file and a saved plan rendered
from one plan cannot disagree about the statements of a change that has a text
form.

### A change without a text form refuses the batch

An operation the engine performs only through a native call has no text form.
In the `TEXT` form the provider then refuses the whole batch with
`RenderResult{complete: false, refusal: {change_id, NO_TEXT_FORM, message}}` and
no unit. The message is one line that names the operation and never a value.
`Render` sets `refusal` only in the `TEXT` form; in the `ANY` form it keeps its
rule that a failure is an error. The host reports the refusal at the change the
plan holds, naming the engine and the operation, and writes no file.

### Where the host asks for text

| Consumer | Form |
| --- | --- |
| Migration files (`migration/generator`), up and down | `TEXT` |
| Data migrations and checkpoint data | `TEXT` (`RenderRowWrites`) |
| `ptah schema render` | `TEXT` |
| `ptah schema diff --format sql` | `TEXT` |
| `ptah schema inspect --format sql` | `TEXT` |
| Compatibility `migrate diff` and `schema diff` | `TEXT` |
| `ptah schema plan`, `ptah schema apply` and saved plans | `ANY` |

A saved plan keeps native units because it runs through the provider that
planned it and binds to that release (ADR 0022, amendments). `schema plan` prints
each statement of a text unit and a native unit's summary, as it does today.

`ptah schema render` and `schema inspect --format sql` of a provider engine do
not depend on `schema.common-table-identity.v1`. The host plans the declaration,
or the inspected schema, against an empty database it knows completely, through
the same comparison and `PlanFeatures` path as a migration, and renders the plan
in the `TEXT` form. An engine without common table identity therefore renders its
own `CREATE` statements.

### Validation

Both transport boundaries refuse:

- a `form` the negotiated description does not allow;
- in the `TEXT` form, a native unit, and a reply that sets both `refusal` and a
  unit;
- a refusal with the reason `NO_TEXT_FORM` in the `ANY` form, a refusal that names
  a change or a write the request does not hold, and one without a message.

### Versioning

`render.text-units` is a compatible addition, part of contract 25.3. A provider
that renders text for some engines lists the feature on those engines only. It
does not raise its minimum contract: an older host never asks for text and keeps
refusing native units where it writes text.

## Alternatives considered

**Render twice and let the host choose.** `Render` would return a text and a
native body for every unit. Every plan would pay for a rendering it does not
use, and a unit with two bodies breaks the rule that a fragment holds one.
Rejected.

**Keep migration files SQL-only and refuse provider engines.** The host refuses a
native unit today, so this costs nothing. The YDB provider's table changes are
native under ADR 0024, so versioned migrations for YDB would stop working at the
flip. Rejected: compatibility never removes a capability, and ADR 0028 already
asks the provider for text.

**Store native envelopes in migration files.** The file would hold an encoded
envelope that the provider executes. A migration file is reviewed and edited as
text, its checksum covers the text, and the compatibility surface hashes it as
the community binary does. An envelope is none of those. Rejected, as ADR 0028
rejected it.

**A separate RPC for text.** `RenderText` would mirror `Render`. It duplicates the
batch rules for one enum's worth of difference. Rejected.

## Consequences

- `RenderRequest`, `RenderRowWritesRequest` and `RenderRefusal` grow one field or
  value each, behind one feature.
- The YDB provider writes YQL for every change it plans natively, or refuses it by
  name. Its text has to match what its native call does; the plugin's parity tests
  run the same change both ways and read the table back.
- `ptah schema render` stops depending on common table identity for provider
  engines, at the cost of one planning pass over an empty catalog.
- A provider that never renders text keeps working everywhere except the text
  consumers, where the host refuses its native units as it does today.

## Acceptance criteria

These are for the implementation. None holds today.

- The SDK carries `RenderRequest.form`, `RenderRowWritesRequest.form` and
  `NO_TEXT_FORM` as contract 25.3, and both boundaries refuse each case under
  "Validation". In-process tests negotiate the feature in both directions, and
  each validator is mutation-checked.
- The fixture provider renders a native operation as text on request, and an SDK
  process test writes the text, executes it through `Execute` and reads the
  result back. Another refuses a change without a text form with no unit.
- The host asks for the `TEXT` form at every consumer in the table above, and a
  test in Ptah's CI writes a migration file through the fixture provider.
- The YDB provider's CI generates migrations for a table alteration, a declared
  row and a comment, applies them with `ptah migrations up`, and reads the result
  back on each YDB line it supports.

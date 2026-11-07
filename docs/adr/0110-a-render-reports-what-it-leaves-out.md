# ADR 0110: A render reports what it leaves out

Status: Proposed

Contract: 25.33

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)
(register G219 and G220)

Relates to: [ADR 0034](0034-a-provider-renders-text-units-where-the-host-writes-text.md)

## Context

A render answers a declaration in one of three ways: it writes a statement for
it, it refuses the whole render, or it goes on without it. The host's model
names the third answer: `renderer.Omission`, carried in the result of a node
render and of a schema render. Callers read it. The inference store and the
comparison probes refuse a rendering that left anything out, the Liquibase
importer refuses a conversion that would lose a declaration, and an embedder of
`engine.Runtime.RenderSchema` gets the list with the statements.

A provider has no field for that answer. `RenderResult` carries units and, in
the `TEXT` form, a refusal. So a provider can write a statement or refuse, and
a declaration its engine cannot keep is either lost in silence or refused
outright.

Ptah's built-in YDB engine at 74de0b65b reported both cases. Its test
`TestRenderSQLReportingOmissions_YDBReportsADroppedTypeModifier` renders a table
with `VARCHAR(255)` and `TIMESTAMP(3)` columns as `Utf8` and `Timestamp` and
reports two omissions: `accounts.email`, `type modifier`, `VARCHAR(255):
length 255`, and `accounts.seen`, `type modifier`, `TIMESTAMP(3): precision 3
(YDB keeps microseconds)`. A `Serial` column declared with a start and an
increment rendered as a table, and the two settings were reported as the
`identity start` and `identity increment` omissions. Through the YDB provider,
the first table renders with no omission (G219), and the provider refuses the
second with `no YQL text form` (G220), because a silent loss is the only other
answer it has.

## Decision

### A complete render carries its omissions

`RenderResult.omissions` lists the declarations a complete `Render` left out.
`RenderOmission` mirrors the host's model without the target, which the
request's engine names: a reason, the owning object's kind and name, the lost
property (empty when the whole object was lost), the declared value where
repeating it helps a reader, and a remedy where one exists on the engine. The
reason is a lower-case token, such as `unsupported`, which a caller branches
on; the other fields are text for a reader. An omission is never a secret: the
detail of a role's password is not a declared value to repeat.

The host turns each omission into a `renderer.Omission` with the engine as its
target, in the result of the node render and of the schema render that asked.
Every caller then treats a provider's omission as it treats a built-in
engine's.

### A two-way feature

`render.omissions` enables the field. The provider lists it, and so does each
engine whose render reports omissions; the provider must offer `Render`. A
host that does not list it receives no omissions, and a render that leaves
something out says nothing, as it does for a host of contract 25.32.

### What the SDK enforces

`ValidateRenderedBatch` refuses omissions the engine or the request's target
does not negotiate, omissions in an incomplete reply or beside a refusal, an
omission without a reason token, a kind or a name, a field that is not one line
of text, and one omission reported twice. `ValidateRenderedRowWrites` refuses
any omission: a row write either renders or refuses. `ValidateDescription`
refuses the engine's feature without the provider's, or on a provider that does
not render.

## Alternatives considered

**Report omissions from planning instead.** A planning service knows the
declared table when it plans its creation, and could return the losses with
its contribution. Rendering is where the host's model reports them, for built-in
engines and for a node render that never plans, and a planned change that
reaches `Render` from a migration file or a saved plan would lose them. The
render is the one place every path crosses.

**Write the loss as a comment in the unit.** PostgreSQL-family built-in
renderers write a `skipped` comment beside a statement. A comment is text for a
reader: a caller that must refuse a lossy rendering cannot read it, the `TEXT`
form would carry a non-statement into a migration file, and a native unit has
no place for one.

**Refuse every declaration the engine cannot keep.** It is the answer the YDB
provider gives a `Serial` with settings without this field. A schema that declares
`VARCHAR(255)` for a portable model would then not render on YDB at all, where
the built-in engine rendered it and said what it dropped.

## Consequences

- A provider can render a declaration its engine keeps in part and say what it
  dropped, as the built-in engines do.
- Callers that refuse a lossy rendering refuse a provider's too.
- Reporting is not exhaustive: an empty list does not establish that a render
  lost nothing, as the host's model already says.

## Acceptance evidence

- SDK unit tests: `TestRenderOmissions_HappyPath`,
  `TestRenderOmission_FailurePath`, `TestRenderOmissionsReply_FailurePath`,
  `TestRenderOmissionsDescription_FailurePath` and
  `TestRowWriteOmissions_FailurePath` in `provider-sdk/provider`.
- The conformance suite validates every `Render` reply with
  `ValidateRenderedBatchReply`, so a narrowed host profile without the feature
  refuses a provider that still reports omissions.
- The host adoption carries a provider's omissions into the result of a schema
  render through a provider process
  (`TestProviderRenderReportsWhatItLeavesOut` in `integration/providercli`).

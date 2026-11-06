---
title: Engines, models and planning
description: What a provider declares about an engine, how its own values travel as models, what a read promises, and how planning work is divided between Ptah and the provider.
type: concept
audience:
  - "go-developer"
  - "integration-developer"
readerQuestion: "How does a provider describe an engine's objects to Ptah, and who plans a change to them?"
goal: "Choose how an engine's objects reach Ptah: through the common model, through provider models, or both, and which planning services that needs."
sourceOfTruth:
  - "provider-sdk/README.md"
  - "provider-sdk/provider"
  - "provider-sdk/modeljson"
  - "internal/providerhost"
generated: false
searchAliases:
  - "provider value model"
  - "provider coverage"
  - "provider planning"
overlaps: []
disposition: keep
---

Ptah models what most engines share: tables, columns, indexes, constraints,
views, sequences, roles and grants. A provider decides how much of its engine
fits that common model and carries the rest as its own models, which Ptah
stores and moves without reading them. Every service a provider adds sits on
one of those two paths, so the choice decides which services the engine needs.

## The engine description

Each engine in `Description.engines` is one entry, and everything Ptah knows
about the engine before it opens a session comes from it:

- **Names.** `id` and `aliases` are the engine names a command can select, and
  the URL schemes of the engine's addresses. A name is lower-case letters,
  digits, `_`, `+` and `-`, starting with a letter. A name that the catalog
  gives to an official engine belongs to that engine's provider, and a scheme
  Ptah reserves for sources, such as `file` or `docker`, is refused.
- **Naming rules.** `default_schema` and `identifiers` say how the engine
  names and compares objects.
- **Features.** `features` lists what the engine offers. A service the
  provider offers for some engines is listed on each engine that supports it.
- **Release lines.** `release_lines` declares the server versions the provider
  knows, each with a support level, and `default_capabilities` states the
  capability facts an offline command plans with.
- **Syntax.** `yaml_source_keys`, `go_source_directives`, the HCL declaration
  and `sql_schema` claim the source syntax the provider reads. A claim is what
  makes Ptah send the provider a fragment of a YAML, Go, HCL or SQL source
  instead of refusing it as unknown.
- **Other declarations.** `query_dialect` tells the query builder how to spell
  statements for the engine. `scope_selections` lets `--include`, `--exclude`
  and `--schema` name the engine's own objects. `lint_rules` lists the rules
  of `lint.migrations`.

The README's
[Captured extension sources (latest development source)](https://github.com/stokaro/ptah/blob/master/provider-sdk/README.md#captured-extension-sources)
section states the source claims, and
[Offline capabilities (latest development source)](https://github.com/stokaro/ptah/blob/master/provider-sdk/README.md#offline-capabilities)
states the release lines.

## Models

A model is a kind of value that the provider owns, such as a YDB topic or the
partitioning settings of a YDB table. Ptah carries its values without reading
them; the provider validates, compares, plans and renders them. A `ValueModel`
declares one model:

- **Kind.** A namespaced name, such as
  `github.com/stokaro/ptah-ydb-plugin/column-properties`, under the provider
  ID.
- **Representation.** `DESIRED` for what a source declares, `OBSERVED` for
  what a read reports, `CHANGE` for a difference a comparison returns, and
  `OPERATION` for a step a plan runs. One kind usually has several
  representations, each its own model.
- **Definition.** A nonempty JSON object that says what the values mean,
  normalized by `modeljson.Canonical`, with its SHA-256 digest. The schema
  version, the encoding and the canonical format complete the identity.
- **Source formats.** The formats a desired value can be written in: YAML, HCL,
  Go annotations or SQL.

A value travels in an `Envelope` that names the provider, the kind, the schema
version, the definition digest, the object or facet it belongs to, and the
payload. A value attached to a table, a column or an index is a facet of it,
and a value that stands alone is a native object.

The definition digest is the model's identity across releases. Ptah binds a
captured claim, such as the `ptah:feature-coverage` header of an exported HCL
file, to the provider ID, the kind, the representation, the schema version and
the digest. A later release reads that claim as long as the definition stays
the same. When the definition text has to change without changing what a
claim means, the engine names the old identity as a predecessor under
`models.predecessors` and `models.own-predecessors`, and documents written
before the change still read. A saved plan is stricter: it also binds the
executable's digest and the semantic identity, so a new build needs a new
plan.

The values of provider models reach the provider through these services:

| Feature | What the provider does |
| --- | --- |
| `values.normalize` | Validates a batch of values and returns each normalized, with a canonical projection. |
| `source.decode` | Reads the source fragments its claims select into desired values. |
| `source.encode` | Writes desired values back as claimed source syntax, or reports each value it cannot write as a loss. |
| `values.convert` | Converts values between the desired and observed models. |
| `models.compare` | Compares desired and observed values and returns changes. |
| `models.plan` | Plans changes as steps of Ptah's plan graph. |

## What a read promises

A read is complete, or the call fails. Beyond the objects it returns, it says
what it knows for each model: a model coverage record per representation, with
regions per namespace. Coverage keeps apart what is complete, absent,
defaulted, not inspected and not representable. An empty list never means that
nothing exists, and a missing coverage record is not evidence that a model has
no objects.

The distinction drives planning. Ptah plans the removal of an object only where
a read covers its model completely, so a provider that cannot read a kind says
so, with a reason, and Ptah leaves that kind alone. A read narrowed to schemas
binds each selector to the namespaces it read and stays inside them. The
README's
[Observed model coverage (latest development source)](https://github.com/stokaro/ptah/blob/master/provider-sdk/README.md#observed-model-coverage)
section states the rules, including reserved storage, which a provider uses to
keep its own history and lock tables out of what a read reports.

## Database contexts and dev databases

An engine whose absolute names depend on the database, as YDB's paths do,
needs the database before it can interpret a value. Under `database.context`,
`ResolveDatabaseContext` turns an address into the database a session selects
and the root that authored absolute references resolve against. It connects to
nothing and reads no credential. Under `values.context`, a value carries that
context, Ptah keeps it through capture and planning, and a value bound to one
database is never applied to another.

Several commands replay or rehearse changes on a database other than the
target. The provider takes part through four features:

- `database.dev` provisions a throwaway database from the `docker://` spec an
  operator types, and removes it when Ptah releases the lease.
- `database.dev-realms` creates a realm of the run's own inside a database the
  operator names, for an engine where only an administrator creates databases.
- `database.dev-cleanup` lists and empties a dev database. Ptah judges the
  database clean when the list is empty.
- `database.dev-replay-guard` judges each statement Ptah is about to replay and
  refuses one whose effects reach outside the dev database.

Ptah decides which database is a dev database; the provider cannot. The
[Dev database cleanup and the replay guard (latest development source)](https://github.com/stokaro/ptah/blob/master/provider-sdk/README.md#dev-database-cleanup-and-the-replay-guard)
section gives the rules.

## Who plans what

Ptah keeps comparison policy, the order of a plan, and the saved plan. A
provider takes part on one or more of these paths:

- **Common tables.** With `schema.common-table-identity.v1` on the engine,
  Ptah compares and plans the engine's tables and columns with its common
  planner and asks `Render` for each change. The engine promises common
  semantics: no implicit objects, no undeclared settings, no extra defaults
  and no type coercion, and it refuses a declaration outside them. The
  [tutorial](../tutorial/) takes this path.
- **Owned tables.** An engine that prepares its own tables lists
  `schema.prepare-tables`, and usually `schema.project-table-creations`,
  `schema.columns` and the observed column, index and primary-key features.
  One lifecycle service of the engine then owns creating, altering and dropping
  a table. An engine cannot take both table paths.
- **Provider models.** `models.compare` returns changes, and `models.plan`
  turns them into operations with dependencies on common steps. Ptah validates
  each contribution and schedules the whole graph; the provider never orders a
  graph of its own. `Render` writes each native operation, and `Execute` runs
  it.
- **Common objects.** Under `schema.common-objects`, views, materialized views,
  sequences, roles, grants and comments travel as common messages. The provider
  reads them, spells declared objects as the engine stores them, and renders
  each change.

A planning reply accounts for every change and every table it was given: each
contributed operation has a receipt, and a refusal carries diagnostics and no
operations. Every command that plans asks for the exact statement style, so a
provider that plans lists `planning.statement-style`. Where Ptah writes text,
such as a migration file or `ptah schema render`, it asks `Render` for the text
form under `render.text-units`, and a change the engine makes only through a
native call is refused there by name.

The README's
[Contextual feature planning (latest development source)](https://github.com/stokaro/ptah/blob/master/provider-sdk/README.md#contextual-feature-planning)
and [Common objects (latest development source)](https://github.com/stokaro/ptah/blob/master/provider-sdk/README.md#common-objects)
sections give the contracts.

## Large schemas

A request carries a captured state for each table, so its size grows with the
schema. Four features of the provider keep a schema of a few thousand tables
in one request. The SDK applies each one on both sides, and a service sees the
same messages either way:

- `models.references` sends a model by its identity, not its whole definition,
  everywhere except in `Describe`.
- `transport.shared-coverage` sends a list of coverage kinds that many tables
  repeat once per message.
- `transport.large-messages` raises the message bound from 4 MiB to 64 MiB.
- `planning.table-outlines` lets a planning service that plans no table
  setting read each table by its subject, its action and the names of its
  states. The service declares this in its planning entry.

A call that grows with the schema runs until the caller's context ends, so a
provider honors cancellation rather than relying on a fixed limit.

## Where this appears

- [How a provider works](../architecture/) covers the process, the contract,
  execution and locks.
- [Provider SDK reference](../sdk-reference/) maps each feature to its service,
  its README section and its decision record.
- [Test a provider](../testing/) checks models and plans with the SDK's
  validators.

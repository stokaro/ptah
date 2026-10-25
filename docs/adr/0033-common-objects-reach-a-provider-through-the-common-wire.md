# ADR 0033: Common objects reach a provider through the common wire

Status: Accepted

Deciders: Ptah maintainers

Issue: [stokaro/ptah#4204](https://github.com/stokaro/ptah/issues/4204)

Relates to: [ADR 0022](0022-provider-values-are-resolved-in-semantic-batches.md),
[ADR 0024](0024-the-provider-owns-table-changes-through-one-lifecycle-service.md),
[ADR 0027](0027-a-provider-reads-the-sql-schema-documents-of-its-own-dialect.md),
[ADR 0031](0031-official-providers-are-installed-separately-and-ptah-pins-no-provider-release.md)

Contract: 25.3, with ADRs 0034 to 0037.

## Context

The host models views, materialized views, sequences, roles, grants and
comments for every engine. The YDB provider has to carry each of them before
`ydb://` can move to `stokaro/ptah-ydb-plugin` with full parity. The provider
contract has no place for any of them outside `ReadSQLSchema`.

What the code does today:

- **The host compares them in shared code.** `migration/schemadiff/internal/compare`
  holds `viewlike.go`, `sequences.go`, `roles.go`, `memberships.go`,
  `grants.go` and `object_comments.go`. Capability keys decide which kinds a
  target stores and reads back, such as `view_comments`. A target supplies the
  rest through `schemacomparison.Rules`: `Views.QueryEqual` and
  `Columns.SequenceChanges`. The built-in YDB engine fills them in
  `internal/ydbcomparison`.
- **The built-in YDB engine knows how YDB holds each one.** `internal/ydbview`
  holds the clause every view needs and the form in which YDB stores a query.
  `internal/ydbacl` maps privileges onto YDB's permission names, users and
  groups. `internal/ydbsequence` names the sequence of a Serial column.
  `internal/ydbcomment` keeps a comment as a user attribute of its table or
  view, because YQL has no comment statement. The reader
  (`internal/dbschema/ydb`: `view.go`, `access.go`, `sequence.go`,
  `comments.go`) and the planner (`internal/planner/dialects/ydb`: `views.go`,
  `access.go`, `object_access.go`, `sequences.go`, `comments.go`) use them.
- **A provider-selected run refuses all of them.** `internal/providerhost/validation.go`
  sends `ValidateSchema` only for a desired schema whose populated fields are
  tables, their children, enums, schemas and feature objects; `onlyFields`
  refuses `Views`, `MaterializedViews`, `Sequences`, `Roles`, `Grants` and
  `RevokedGrants`. `internal/providerhost/render.go` turns tables and native
  operations into `Change` messages and refuses every other AST node,
  `CreateViewNode`, `GrantPrivilegeNode` and `ObjectCommentNode` among them.
  `internal/providerhost/inspect.go` fills the tables, the feature objects and
  the reserved names of a `catalog.Database` and nothing else: `Schema` has no
  field for these objects, and `Table` and `Column` carry no comment.
- **The declared shapes exist.** ADR 0027 gave `SQLSchema` the messages
  `SQLView`, `SQLSequence`, `SQLRole` and `SQLGrant`, and the host maps them
  into `schemamodel` in `internal/providerhost/sql_schema_model.go`. `SQLGrant`
  has no column list, and `SQLRole` cannot say that a password is declared.

The owner decided the direction: these objects reach a provider through a common
wire in the contract, the way tables do, and not through models each provider
defines for itself.

## Decision

### One feature, one declaration

The feature `schema.common-objects` switches the wire on. It is a two-way
feature: it adds fields to existing messages, so a host lists it in
`DescribeRequest.host_features`, and the provider lists it in
`Description.features` and in each supporting engine's features. An engine that
lists it declares the kinds it carries:

```proto
// Engine gains: CommonObjectSupport common_objects = 16;
message CommonObjectSupport {
  enum Kind {
    KIND_UNSPECIFIED = 0;
    VIEW = 1;
    MATERIALIZED_VIEW = 2;
    SEQUENCE = 3;
    ROLE = 4;
    GRANT = 5;
    COMMENT = 6;
  }
  repeated Kind kinds = 1;
}
```

The SDK drops the declaration, and refuses every field below, when either side
does not list the feature. A kind appears once. A host of contract 25.2 or older
never sees the declaration, and the provider then gets no common object.

### Who owns what

The split is the one tables have. The host keeps what is the same on every
engine; the provider keeps what the engine decides.

| Step | Host | Provider |
| --- | --- | --- |
| Read | Maps the reply into `catalog.Database` | `Inspect` reports every object of a declared kind |
| Validate | Refuses a kind the engine does not declare | `ValidateSchema` judges the declared objects |
| Spell | Keeps the authored declaration | `PrepareCommonObjects` returns the engine's spelling |
| Compare | Pairs, compares, applies coverage and policy | Nothing |
| Plan | Orders steps in the shared graph | `PlanFeatures` sees the steps as context |
| Render | Sends typed changes | `Render` writes one unit per change |

The comparison stays in `migration/schemadiff`, so a difference, a refusal and
its words are the same on every engine. The engine's facts reach it as data:
capability keys from the session, and the prepared declaration.

### Declared objects

The declared shapes are the messages `ReadSQLSchema` returns. A declared view,
sequence, role or grant has one shape wherever it travels:

```proto
message DeclaredCommonObjects {
  repeated SQLView views = 1;          // materialized views set materialized
  repeated SQLSequence sequences = 2;
  repeated SQLRole roles = 3;
  repeated SQLGrant grants = 4;        // revoked grants set revoked
}
// SQLRole gains: bool password_declared = 11;
// SQLGrant gains: repeated string columns = 7;
```

`password_declared` says that the declaration gives the role a password. The
password itself never travels in a declaration. `columns` limits a grant to
those columns of its table target, as `schemamodel.Grant.Columns` does. Both
fields are additions to existing messages, so a provider sets them in a
`SQLSchema` reply only when the host lists the feature.

A declared comment stays where the declaration writes it: in `comment` of
`DeclaredTable`, `DeclaredColumn`, `DeclaredIndex`, `DeclaredNamespace`,
`SQLView`, `SQLSequence` and `SQLRole`.

`ValidateSchemaRequest` gains `DeclaredCommonObjects common_objects = 12`. The
host fills it from `schemamodel.Database` and refuses, by name and before any
call, a populated kind the engine does not declare.

### Observed objects

`Schema`, the reply of `Inspect`, gains `ObservedCommonObjects common_objects = 10`.
The observed shapes follow `catalog.Database`, as `ObservedTable` follows its
table, because a read reports facts a declaration does not have:

```proto
message ObservedCommonObjects {
  repeated ObservedView views = 1;
  repeated ObservedSequence sequences = 2;
  repeated ObservedRole roles = 3;
  repeated ObservedGrant grants = 4;
  repeated ObjectComment comments = 5;
  // One entry for each kind the engine declares.
  repeated CommonObjectCoverage coverage = 6;
  // Roles the server holds that this read does not describe.
  repeated string roles_out_of_scope = 7;
}
message CommonObjectCoverage {
  CommonObjectSupport.Kind kind = 1;
  ModelKnowledge knowledge = 2;        // COMPLETE, or UNINSPECTED with a reason
}
message ObservedView {
  Reference identity = 1;              // kind "view" or "materialized_view"
  string body = 2;                     // as the server stores it
  string check_option = 3;
  repeated string attributes = 4;
}
message ObservedSequence {
  Reference identity = 1;              // kind "sequence"
  string data_type = 2;
  optional int64 start = 3;
  optional int64 increment = 4;
  optional int64 min_value = 5;
  optional int64 max_value = 6;
  optional int64 cache = 7;
  bool cycle = 8;
  // The owning column, path [table] and name, for a sequence a column owns.
  Reference owned_by = 9;
}
message ObservedRole {
  string name = 1;
  bool login = 2;
  bool superuser = 3;
  bool create_db = 4;
  bool create_role = 5;
  bool inherit = 6;
  bool replication = 7;
  bool group = 8;
  repeated string member_of = 9;
  enum Password { PASSWORD_UNKNOWN = 0; PASSWORD_NONE = 1; PASSWORD_SET = 2; }
  Password password = 10;
}
message ObservedGrant {                // one privilege, as catalog.Grant holds it
  string role = 1;
  string privilege = 2;
  Reference target = 3;                // kind "table", "schema", "database", ...
  bool with_grant_option = 4;
  string granted_by = 5;
  repeated string columns = 6;
  bool implicit = 7;
}
message ObjectComment {
  // kind "table", "view", "materialized_view", "sequence", "schema" or "role";
  // a column or an index has path [table] and its own name.
  Reference object = 1;
  string text = 2;
}
```

Coverage is per kind and is required: `COMPLETE` says the reply lists every object
of the kind in the inspected scope, and `UNINSPECTED` says the read did not look,
with a reason. The host maps `UNINSPECTED` to a `NotDescribed` entry of the kind,
so nothing it did not read is planned for removal. A reply that lists an object
of an undeclared or uninspected kind, omits a declared kind's coverage, or
lists one twice is refused. An observed comment holds text; an object without a
comment has no entry. A sequence a Serial column owns is reported with
`owned_by`, so the host compares its start and increment without knowing how
the engine names it.

### Comparison: the provider spells, the host compares

A query, a privilege or a type name reads differently on each engine. YDB keeps
a view's query as its tokens joined by single spaces, and calls the privilege
`SELECT` `ydb.granular.select_row`. The host cannot fold those itself, and a
value interface that called a provider would hide its failures (ADR 0022). So the
provider answers in one offline batch before the comparison:

```proto
rpc PrepareCommonObjects(PrepareCommonObjectsRequest) returns (PreparedCommonObjects);

message PrepareCommonObjectsRequest {
  string engine = 1;
  ComparisonIdentifiers identifiers = 2;
  map<string, bool> capabilities = 3;
  repeated string target_features = 4;
  DeclaredCommonObjects declared = 5;
}
message PreparedCommonObjects {
  bool complete = 1;
  // The declaration in the engine's spelling: each collection has the
  // request's length and order, and each entry keeps its identity.
  DeclaredCommonObjects prepared = 2;
}
```

The provider may change how a value is spelled: a view body, a sequence type, a
grant's privilege names. It may not change what an entry is: a schema, a name, a
grant's role and target, `materialized`, `revoked` and `group` stay as declared,
and a privilege list is never empty. The host compares the prepared declaration
with the observed objects and keeps the authored one for rendering and for
saved plans, as it keeps authored tables beside `PrepareTables`.

`CompareModels` gains no field. Native object comparisons keep the context they
have, `declared_roles` and `declared_relations` among it.

### Planning and rendering

The host's common planner plans common objects as it plans them for its own
engines, and places each step in the shared graph with its effects. Two
additions carry what a provider needs:

- `PlanFeaturesRequest` gains `DeclaredCommonObjects desired_common = 13` and
  `ObservedCommonObjects current_common = 14`, so a parent planner sees the
  grants and comments of a table it alters or drops.
- `CommonPlanningStep` gains `CommonObjectChange common_object = 5`, the operand
  of a step that changes a common object, as `added_column` is for a column. A
  provider orders its operations against such steps with dependencies, or
  rewrites them, under the rules `PlanFeatures` already has.

`Change` gains `CommonObjectChange common_object = 8`. The host maps the AST
nodes `render.go` refuses today onto it:

```proto
message CommonObjectChange {
  oneof operation {
    ViewCreation create_view = 1;
    Reference drop_view = 2;
    SQLSequence create_sequence = 3;
    SequenceAlteration alter_sequence = 4;
    Reference drop_sequence = 5;
    RoleCreation create_role = 6;
    RoleAlteration alter_role = 7;
    Reference drop_role = 8;
    SQLGrant grant = 9;
    SQLGrant revoke = 10;              // with_grant_option revokes the option only
    RoleMembership add_member = 11;
    RoleMembership remove_member = 12;
    ObjectComment set_comment = 13;    // empty text removes the comment
  }
}
message ViewCreation { SQLView view = 1; bool replace = 2; }
message SequenceAlteration {
  Reference sequence = 1;              // the observed identity
  SQLSequence desired = 2;
  repeated string changed = 3;         // "start", "increment", "min_value", ...
  bool restart = 4;
}
message RoleCreation { SQLRole role = 1; string password = 2; }
message RoleAlteration {
  SQLRole desired = 1;
  repeated string changed = 2;         // "login", "password", "superuser", ...
  string password = 3;
}
message RoleMembership { string group = 1; string member = 2; }
```

`Change.object` names the subject of the change. The provider renders one unit
per change, as for every other change, or refuses the batch. It decides how its
engine spells each one: YQL text for a YDB grant, and a native unit or a
statement its own executor reads for a comment YDB keeps as an attribute.

### Refusals

- The host refuses a declaration of a kind the selected engine does not declare,
  naming the kind and the engine, before it sends anything. It never drops one.
- A provider refuses, with a diagnostic of `ValidateSchema`, a declared object it
  recognizes and cannot hold, and names it. It does not skip one.
- Both transport boundaries refuse a request or a reply that uses an undeclared
  kind, an empty name, a grant without a role, a privilege or a target, a column
  or index comment without its table, a role that is a member of itself, a
  duplicate identity under the engine's identifier rules, and a prepared entry
  that changed its identity or the length of its collection.

### Atomicity and errors

`PrepareCommonObjects` is offline and atomic, like `PrepareTables`: a complete
reply or an error, and a canceled call returns nothing. `Inspect` keeps its rule
that an incomplete read is an error, never an empty catalog. A refused render
batch exposes no prefix. Execution, transactions and uncertain outcomes are
those of `Execute`.

### Trust

A role's password travels only in `RoleCreation` and `RoleAlteration`, which
`Render` receives because the unit it writes has to set it. No validation,
preparation, inspection or planning message carries one. A diagnostic and a
refusal never repeat a password or a view body.

### Versioning

The wire is a compatible addition, so it is part of contract 25.3 (ADR 0031):
`provider.ContractMinor` becomes 3 and `provider.Revision` stays 25. A host or a
provider of an older minor exchanges the messages of its own minor. A provider
that carries common objects does not raise its minimum contract for them: a host
that does not list the feature refuses these objects as it does today.

## Alternatives considered

**Provider-native models for each object.** The YDB provider would declare its
own view, role and grant models, as it declares a topic. No contract change is
needed. Every provider would then reimplement the comparison of objects the host
already compares for every engine, a refusal would read differently per engine,
and a grant on a table could not be compared with a grant the host reads from
another source. Rejected by the owner.

**Let the provider compare common objects in `CompareModels`.** The provider
would receive both sides and return typed changes. It puts the comparison of a
common object in every provider, which is the cost above, and the capability
gating in `object_comments.go` would move with it. Rejected.

**Comparison rules as data.** The engine would declare, for example, that a view
query compares by its tokens. It covers the view and not the privilege names or
a sequence's defaults, and each new rule would be a host change. Rejected; a
preparation call answers every kind in one shape.

**Comments as fields of `Table` and `Column`.** The inspection reply would carry
a comment on each object that has one. Views, sequences, roles and schemas would
need the same field, and an engine that keeps a column comment on its table, as
YDB does, would still report it per column. One list keyed by `Reference` serves
every kind. Chosen for the observed side; the declared side keeps the fields it
has.

## Consequences

- The SDK grows one RPC, two engine and reply declarations, the observed
  messages and the render operations. It speaks contract 25.3.
- The host gains an adapter for each step: the inspection mapping into
  `catalog.Database`, the validation and preparation captures, the planning
  context, and the AST mapping in `render.go`.
- A comparison over common objects costs one more offline call. It is one batch
  for the whole schema.
- The YDB provider takes over what `internal/ydbview`, `internal/ydbacl`,
  `internal/ydbsequence` and `internal/ydbcomment` know, with the reader and
  planner parts that use them. They are deleted from the host at the flip.
- Functions, row-level security and triggers outside a table are not part of this
  wire. A host that holds them for an engine with this feature still refuses them
  by name.

## Acceptance criteria

These are for the implementation. None holds today.

- `provider.proto`, the SDK interfaces, both transport boundaries and the
  validators carry the wire as contract 25.3, with `provider.ContractMinor` at 3,
  and refuse every case under "Refusals".
- In-process SDK tests negotiate the feature in both directions: a host that does
  not list it gets no declaration, and a provider that sends a common object
  anyway is refused. Each validator is mutation-checked.
- The fixture provider carries views, grants and comments on SQLite, and SDK
  process tests in Ptah's CI inspect them, prepare a declaration, render each
  operation and read the result back.
- The host adapter plans and applies a view, a role, a grant and a comment
  through the fixture provider in Ptah's CI, and a re-run plans nothing.
- The YDB provider implements the wire. Its CI runs the YDB parity tests for
  views, users and groups, permissions, Serial sequences and comments against a
  current Ptah, on the YDB lines it supports.
- `docs/site/src/content/docs/extend/executable-providers.md` documents the
  feature and its kinds.

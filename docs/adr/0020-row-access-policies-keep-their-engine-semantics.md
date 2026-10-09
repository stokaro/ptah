# ADR 0020: Row-access policies keep their engine semantics

- Status: proposed
- Deciders: Ptah maintainers
- Issue: [#4140](https://github.com/stokaro/ptah/issues/4140)
- Process-boundary requirements: [#4204](https://github.com/stokaro/ptah/issues/4204)

This record defines the ownership and extraction requirements for row-level
security (RLS). It addresses maintainers moving existing policy behavior into
feature providers. The model names below describe the target design; this record
does not claim that the extraction or its acceptance tests have shipped.

## Context

The source observations below use commit
`310b3d4fb4b59fd43eb7b86db800d3be341a701c`.

| Current path | Observation | Consequence for extraction |
| --- | --- | --- |
| `core/schemamodel/types.go`, `catalog/types.go` | `RLSPolicy` carries PostgreSQL command, role, predicate, and composition fields. Table enablement and force state use separate common fields. | The shared name does not establish shared semantics. |
| `internal/dbschema/mssql/rls.go` | The reader groups by policy and target table, discards the policy schema, and maps both UPDATE timings to one value. Repeated block predicates overwrite the previous expression. | One schema-scoped policy cannot be reconstructed from independent table policies without losing state. |
| `internal/dbschema/clickhouse/rowpolicy.go` | The query reads role selection and its exceptions, but does not select `is_restrictive`. | A restrictive observation must not become a permissive declaration or a no-op comparison. |
| `migration/safety/safety.go` | The common policy buckets classify additions as safe and removals as destructive. | These labels do not establish the access effect of a specific policy and its siblings. |
| `core/schemacapture/tables.go`, `core/schemaext/objects.go` | A table capture includes feature objects whose structured parent identifies that table. | Direct parentage cannot capture a schema-level policy that refers to several tables. |
| `core/plangraph/types.go` | A step carries object effects, dependencies, and a transaction requirement. Ordering does not combine separate steps into a transaction. | An ordered drop and create are not evidence of an atomic policy replacement. |

The server contracts also differ. PostgreSQL policy identity includes the table;
command selection, role selection, and permissive/restrictive composition affect
its meaning. Table enablement and forced enforcement are separate state.
See [CREATE POLICY](https://www.postgresql.org/docs/18/sql-createpolicy.html) and
[row security](https://www.postgresql.org/docs/18/ddl-rowsecurity.html).

SQL Server has a schema-scoped security policy containing predicate bindings.
Bindings refer to tables and inline table-valued functions. Filter predicates,
block timing, policy state, schema binding, and replication behavior are distinct
properties. A policy can bind several tables. See
[CREATE SECURITY POLICY](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-security-policy-transact-sql?view=sql-server-ver17).

ClickHouse supplies a SELECT filter and its own role selection and composition
rules. Similar syntax does not establish PostgreSQL command or default-access
semantics. Server settings can affect unmatched users, so effective access is not
a fact available from policy text alone. See
[CREATE ROW POLICY](https://clickhouse.com/docs/reference/statements/create/row-policy).

## Decision

### Semantic models own identity and state

Use optional typed models with separate kind identifiers and codecs:

| Model | Identity and placement | State owned by the model |
| --- | --- | --- |
| `feature/pgpolicy.Policy` | A feature object identified by table and policy name. | Command, structured role selectors, optional USING and WITH CHECK expressions, composition, and comment. |
| `feature/pgpolicy.TableState` | A facet of the table. | Enabled and forced flags, independently. |
| `dialect/mssql/mssqlschema.SecurityPolicy` | One schema-level feature object, with no artificial table parent. | Enabled state, schema binding, replication behavior, and all predicate bindings. Each binding retains its table, function invocation, predicate type, and exact operation timing. |
| `dialect/clickhouse/chschema.RowPolicy` | A feature object identified by the supported table target and policy name. | SELECT filter state, composition, role selection including ALL and exclusions, and supported target-specific attributes. |

The names do not require one shared Go struct for authored and inspected state.
An omitted declaration requests a documented default; an observation records
what the reader established. Missing WITH CHECK, a supplied expression, and an
unreadable expression remain distinct. Role keywords and quoted role names must
not collapse into one comma-separated string.

`pgpolicy` depends on neutral value and identity contracts. It does not import
PostgreSQL rendering, a driver, or runtime composition. Another target may select
this model only when its adapter implements the same contract and checks the
operations it supports. A broad `RowLevelSecurity` capability is insufficient.

PostgreSQL, SQL Server, and ClickHouse models do not translate into each other
implicitly. The existing source spellings need explicit frontend decisions:
retain a spelling only with an unambiguous model selection and lossless meaning.
Otherwise replace it with a model-qualified declaration and a clear diagnostic.
Pre-v1 compatibility does not justify retaining the overloaded model or a second
writable representation. Native and compatibility frontends select the same
shared capability; compatibility code does not become its owner.

Unsupported server variants remain explicit. For example, a table-target model
cannot silently flatten a database-wide ClickHouse policy into the tables the
reader happened to enumerate. Extending supported variants requires an owner
model change and measured tests, not a new field in the common policy struct.

### Coverage belongs to the source and the named subject

Frontends and readers enroll only the models and scopes they can describe.
Provider registration grants no authority to remove an undeclared policy.
Known absence requires complete enumeration for the relevant namespace; a
filtered or permission-limited read does not establish absence elsewhere.

A reader that cannot capture a binding, composition mode, or enforcement flag
records an unrepresentable or unknown observation for its subject. It never
fills the missing state with a permissive policy or an enabled default.
Comparison may retain an unmanaged current definition in the effective desired
snapshot, but destructive parent operations need enough captured state to
preserve it. Unknown relevant policy state blocks replacement or rebuild.

### Dependencies are separate from exclusive parentage

A PostgreSQL policy belongs to one table. A SQL Server security policy references
several tables and functions while remaining one independently named object.
Do not duplicate that object under every table or keep a second mutable index.

Before scope selection and planning, the selected owner supplies typed reference
relationships for its captured objects. The host derives reverse lookups from
that immutable batch. Relationship records retain the referencing subject,
referenced subject, and whether the relationship is ownership or dependency.
Opaque SQL text is not a complete dependency declaration; unresolved relevant
references require an explicit refusal before changing their targets.

The current direct-child capture needs a neutral extension for these
relationships. It must carry complete related objects and their coverage into
parent assessment, including unchanged objects. Adding a SQL Server branch to
generic table capture would leave the ownership problem intact.

Selecting one table must not truncate a policy that also binds another table.
Include the complete policy and required context, or refuse the partial scope.
Deleting a selected table does not authorize deleting the other bindings.
The owner plans a supported binding change or refuses before emitting output.
This rule applies to export, reverse generation, and parent rebuilds as well.

Common steps expose structured effects for tables, columns, functions, and roles
where policy planning needs them. Providers use these accepted steps to add
dependency edges or request validated rewrites. They do not rediscover changes
from mutable schemas or append private SQL after the common graph is scheduled.
Uncaptured prerequisites remain unknown; they do not justify a fresh live read
inside planning.

### Access effects require owner assessment

Keep access effects separate from data loss and object lifecycle. A policy
creation can expand permitted access; a removal can narrow it. Neither verb
proves the security effect without the model, enforcement state, and relevant
sibling policies.

The owner reports whether a change can widen access, narrow access, leave it
unchanged, or has unknown effect, with a reason. This assessment needs a neutral
contract before policy extraction; it must survive both change and operation
codecs and reach native and compatibility reports. Until that contract is
available, unknown access effects require conservative existing safety handling,
never a default-safe result.

Changing an arbitrary predicate has unknown access effect unless the adapter
has a specific proven rule. String equality can establish unchanged text;
it does not prove logical equivalence. Disabling PostgreSQL enforcement or
removing forced enforcement can widen access even if policy objects stay intact.

Replacement planning validates the complete replacement before accepting a
drop. A transaction-required operation must reach an executor that enforces its
atomic boundary; dependency order alone is insufficient. Where the target
cannot provide the required boundary, report and gate any protection gap or
refuse the strategy. Do not present a drop/create sequence as uninterrupted
enforcement.

Reverse generation restores captured policy configuration where supported.
It cannot undo access already granted or recover information exposed while a
policy was absent. Reverse diagnostics retain this limitation separately from
whether the previous configuration is reconstructible.

### Local batches remain suitable for a process boundary

Model, change, and operation codecs use explicit versioned records. Preserve
qualified identities, optional fields, binding lists, coverage, access effects,
and reverse limitations. Do not serialize Go AST nodes, package names, or
callbacks. Deterministic encoding sorts sets while preserving meaningful order.

Comparison, relationship discovery, parent assessment, and planning operate on
complete owner batches with frozen target facts. A process adapter maps those
contracts into protocol records under #4204; it does not issue one request per
predicate or implement its own planner, approval rules, or migration history.

## Acceptance before removing the common RLS paths

The extraction under #4140 must establish these results through the shipping
pipeline, with the native and compatibility adapters sharing the same owner:

- Clone, explicit codec, and fingerprint tests retain optional values, quoted
  identities, composition, role selectors, independent flags, and all bindings.
- PostgreSQL tests distinguish policy names on different tables, FORCE from
  ENABLE, and permissive from restrictive policies. Read, plan, apply, reread,
  and reverse tests use real enforcement behavior as well as schema metadata.
- SQL Server tests retain a disabled multi-table policy, qualified table and
  policy names, and distinct BEFORE UPDATE and AFTER UPDATE bindings. Partial
  selection and rebuilding one bound table preserve the other bindings or refuse.
- ClickHouse tests retain restrictive observations and ALL EXCEPT selectors.
  Unsupported targets or settings cannot pass as PostgreSQL-equivalent state.
- Incomplete inspection, malformed payloads, unsupported targets, and missing
  dependency context return no executable prefix or fabricated empty namespace.
- Changes to an unchanged policy's referenced table, column, function, or role
  reach owner assessment. Competing writers and dependency cycles are refused.
- Safety tests distinguish access expansion, restriction, and unknown effects.
  Replacement tests exercise failure before and during execution, including the
  actual transaction boundary. A reversible definition is not reported as
  recovery from prior disclosure.
- An external-module fixture exercises a multi-table feature object through
  public contracts without built-in providers or engine imports in core.
  Source-derived routing inventories account for every removed concrete node.

This record identifies missing relationship and access-assessment contracts;
it does not close those implementation gaps. Do not settle the extension API
using only the existing table-child and standalone coordination examples.

## Alternatives and consequences

Keeping one enlarged `RLSPolicy` struct would preserve current call sites, but
allow meaningless combinations and require dialect branches in common code.
A single predicate-only model would simplify transport by discarding command,
role, timing, and enforcement guarantees. Neither satisfies #4140.

Separate owner models require explicit frontend changes, codecs, and additional
dependency capture. They also require live tests per supported adapter; sharing
a security category cannot replace those tests. The accepted cost is more
precise models and owner-specific validation, with one common graph, safety
boundary, and execution policy.

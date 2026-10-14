# What every desired-schema field is for

A fact an author declares can disappear on the way to SQL, and the render still
exits 0. Five repairs for that shape landed in one week across four package
areas: `internal/deporder`, the ClickHouse renderer,
`internal/convert/dbschematogo`, and the model-to-AST lowering now in
`internal/modelast`. Each path walked the model field by field and omitted a
field it had not been taught about.

This document is the answer to "which fields could that happen to next". Every
field reachable from `core/schemamodel.Database` carries one disposition, and
the ones that reach SQL are measured rather than asserted: the census removes a
field from a fixture, renders the schema again on every release line
[`internal/capabilityprobe`](../internal/capabilityprobe) declares, and reports
where the output moved. A field whose removal changes nothing anywhere is a
field nothing reads.

The register is generated from [`internal/schemacensus`](../internal/schemacensus).
`scripts/check-docsync.sh` fails when this page and that package disagree, and
`--write` regenerates it.

## What it does not prove

It proves a field is **read**, not that what it produces is right. An ablation
that moves the output shows the renderer consulted the field; whether the SQL
that came out is correct is what the per-dialect tests answer.

The ablation measures the render path. The comparison path is the round trip's,
below. A field read only by the planner is classified as such and carries its
reason, and nothing here watches the planner run.

## The round trip

A comparison reads one side through a conversion. `ptah schema diff --from
a.hcl --to a.hcl` converts the `--from` document to the catalog shape a
database read produces (`internal/convert/goschematodb`), and every command
that turns a database back into a declaration takes the other conversion
(`internal/convert/dbschematogo`). A field either conversion drops makes a
schema differ from itself, and the plan is a migration nobody asked for.

The round trip asks this of every census fixture on every dialect whose render
accepts it, in both directions:

- **Preservation.** The fixture compared with its own conversion plans nothing:
  Diff(S, ToCatalog(S)) and Diff(FromCatalog(ToCatalog(S)), ToCatalog(S)) are
  empty.
- **Discrimination.** An empty plan proves nothing about a field both the
  conversion and the comparison drop. So each field disposed `ddl` or
  `comparison` is changed on the converted side alone (removed, perturbed, or
  set to a valid alternate), and the plan must change. A `ddl` field is required
  on each dialect whose rendered statements read it, so a dialect-specific drop
  is caught.

Each dialect runs on the declared release line with the most capabilities,
with every YDB feature flag on, through the comparison a connected command
runs. Owner models take part through their registered conversion services, so
a facet or an object an owner adds is covered the day it is.

The catalog shapes the reverse direction starts from are the ones the forward
conversion produces. A field only a database read fills is outside it.

The gate holds four registers, all generated below: the losses, the gaps, the
exemptions, and the compared fields no fixture lets the round trip ask about.
Each is exact in both directions. A new loss or gap fails the build, and so
does a repaired one until its entry is removed in the same change.

## The gate

Three properties fail the build in `internal/schemacensus`:

- a field the model has and the register does not — so adding a field to
  `schemamodel` is a decision rather than an omission;
- a field exempted from rendering with no reason written down;
- a field declared to reach SQL that no ablation can be seen through, unless it
  carries a gap naming the issue that tracks the repair.

The last one runs in both directions. A gap the census **can** now see also
fails, so repairing one of the fields below fails the build until its entry is
reclassified in the same change.

<!-- BEGIN GENERATED FIELD DISPOSITIONS -->
623 fields are reachable from the desired schema, and each one carries
exactly one disposition.

| Disposition | Fields | What it means |
| --- | --- | --- |
| `ddl` | 514 | reaches rendered SQL on at least one target |
| `comparison` | 34 | read when two schemas are compared, and written into no statement |
| `planning` | 10 | read while a change set is assembled or ordered |
| `derived` | 10 | computed from other fields rather than authored |
| `source` | 31 | identifies the source text the declaration was read from |
| `export` | 13 | what a generated document carries, or reports that it cannot |
| `data` | 11 | reference or seed rows, which are not DDL |

### Fields that should render and do not

None.

### Round trips that plan a change

5 recorded losses. Each is a fixture that, compared with its own conversion, plans a
migration nobody asked for.

| Fixtures | Dialects | Directions | Why |
| --- | --- | --- | --- |
| `function-procedure` | `cockroachdb`, `mariadb`, `mysql`, `oracle`, `postgres`, `sqlserver`, `yugabytedb` | to-catalog | goschematodb drops a routine's kind, so a procedure reads as a function and the plan creates the procedure beside a DROP FUNCTION |
| `grant-routine` | `cockroachdb`, `postgres`, `yugabytedb` | to-catalog | goschematodb drops a routine's kind, so a procedure reads as a function and the plan creates the procedure beside a DROP FUNCTION |
| `grant-routine` | `postgres` | from-catalog | dbschematogo drops the EXECUTE that PUBLIC holds on a procedure, so the converted-back declaration revokes it |
| `function-procedure` | `clickhouse`, `spanner`, `sqlite` | to-catalog | the comparison lists a routine on a target that has none, so the plan carries notes about it or is refused |
| `grant-routine` | `spanner`, `sqlite` | to-catalog | the comparison lists a routine on a target that has none, so the plan carries notes about it or is refused |

### Compared fields a change of which plans nothing

63 known gaps: a conversion or the comparison drops each one on the dialects named.

| Field | Dialects | Directions | Why |
| --- | --- | --- | --- |
| `mssqlproperty.Property.Column` | `sqlserver` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `mssqlproperty.Property.Name` | `sqlserver` | both | goschematodb drops it: the catalog is the same with and without it |
| `mssqlproperty.Property.Schema` | `sqlserver` | both | goschematodb drops it: the catalog is the same with and without it |
| `mssqlproperty.Property.Table` | `sqlserver` | both | goschematodb drops it: the catalog is the same with and without it |
| `mysqlschema.DesiredColumnSettings.Charset` | `mariadb`, `mysql` | both | goschematodb drops it: the catalog is the same with and without it |
| `mysqlschema.DesiredColumnSettings.OnUpdate` | `mariadb`, `mysql` | both | goschematodb drops it: the catalog is the same with and without it |
| `mysqlschema.DesiredIndex.Parser` | `mariadb`, `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `mysqlschema.DesiredIndexBlockSize.KeyBlockSize` | `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `mysqlschema.DesiredTable.AutoIncrement` | `mariadb`, `mysql` | both | goschematodb drops it: the catalog is the same with and without it |
| `mysqlschema.DesiredTable.Charset` | `mariadb`, `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `mysqlschema.DesiredTable.Engine` | `mariadb`, `mysql` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Constraint.Facets` | `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Constraint.UsingMethod` | `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Database.Schemas` | `clickhouse`, `mariadb`, `mysql`, `spanner`, `sqlserver` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.EmbeddedField.Overrides` | `mysql` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.EmbeddedField.Type` | `cockroachdb`, `sqlite` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Field.AutoInc` | `mariadb`, `mysql`, `oracle`, `sqlite`, `sqlserver` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Field.Collate` | `mariadb`, `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Field.DefaultSet` | `clickhouse`, `cockroachdb`, `mariadb`, `mysql`, `oracle`, `postgres`, `spanner`, `sqlite`, `sqlserver`, `ydb`, `yugabytedb` | from-catalog | dbschematogo drops it: the converted-back declaration is the same with and without it |
| `schemamodel.Field.DefaultSet` | `clickhouse`, `cockroachdb`, `mariadb`, `mysql`, `oracle`, `postgres`, `spanner`, `sqlite`, `sqlserver`, `ydb`, `yugabytedb` | to-catalog | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Field.Facets` | `mariadb`, `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Field.GeneratedExpression` | `oracle` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Field.IdentityGeneration` | `cockroachdb`, `mariadb`, `mysql`, `oracle`, `postgres`, `spanner`, `sqlserver`, `yugabytedb` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Field.IdentityIncrement` | `sqlserver` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Field.IdentityOptions` | `cockroachdb`, `postgres`, `spanner`, `yugabytedb` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Field.IdentityStart` | `sqlserver` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Field.Overrides` | `mysql` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Field.TypeIsDeclaredText` | `clickhouse`, `sqlite`, `sqlserver` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Field.TypeRawSQL` | `clickhouse`, `sqlserver` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Function.Dialects` | `clickhouse`, `oracle`, `spanner`, `sqlite` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Function.Dialects` | `cockroachdb`, `mariadb`, `mysql`, `sqlserver`, `ydb`, `yugabytedb` | to-catalog | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Function.Kind` | `cockroachdb`, `postgres`, `yugabytedb` | from-catalog | the conversions drop a routine's kind; the other direction is a recorded loss |
| `schemamodel.Function.Kind` | `mariadb`, `mysql`, `sqlserver` | from-catalog | the conversions drop a routine's kind; the other direction is a recorded loss |
| `schemamodel.Index.Facets` | `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Index.Fields` | `clickhouse`, `oracle` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Index.Parts` | `clickhouse`, `oracle` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Index.Unique` | `oracle` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.IndexPart.Desc` | `oracle` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.IndexPart.Expr` | `clickhouse`, `oracle`, `sqlserver` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.IndexPart.Name` | `clickhouse`, `oracle` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.MaterializedView.Dialects` | `clickhouse`, `mariadb`, `mysql`, `oracle`, `sqlite`, `sqlserver`, `ydb` | to-catalog | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.MaterializedView.Dialects` | `spanner` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.PrimaryKeyPart.Desc` | `mariadb`, `mysql`, `sqlserver` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.PrimaryKeyPart.Name` | `clickhouse` | to-catalog | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.PrimaryKeyPart.Name` | `cockroachdb`, `mariadb`, `mysql`, `oracle`, `postgres`, `spanner`, `sqlite`, `sqlserver`, `yugabytedb` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.PrimaryKeyPart.Prefix` | `mariadb`, `mysql` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Schema.Name` | `clickhouse`, `cockroachdb`, `mariadb`, `mysql`, `postgres`, `spanner`, `sqlserver`, `yugabytedb` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Sequence.Dialects` | `cockroachdb`, `mariadb`, `oracle`, `spanner`, `sqlserver`, `ydb`, `yugabytedb` | to-catalog | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Table.Collate` | `mariadb`, `mysql` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Table.Engine` | `mariadb`, `mysql` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Table.Facets` | `mariadb`, `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Table.Overrides` | `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Table.PrimaryKeyFacets` | `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Table.PrimaryKeyMethod` | `mysql` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Table.PrimaryKeyName` | `cockroachdb`, `oracle`, `postgres`, `spanner`, `sqlserver`, `yugabytedb` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Table.PrimaryKeyParts` | `mariadb`, `mysql`, `sqlserver` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Trigger.Dialects` | `clickhouse`, `spanner` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.Trigger.Dialects` | `cockroachdb`, `mariadb`, `mysql`, `oracle`, `sqlite`, `sqlserver`, `ydb`, `yugabytedb` | to-catalog | both conversions carry it, and the comparison plans nothing when it changes |
| `schemamodel.Trigger.ExecuteFunction` | `cockroachdb`, `postgres`, `yugabytedb` | both | goschematodb drops it: the catalog is the same with and without it |
| `schemamodel.View.Dialects` | `clickhouse`, `mariadb`, `mysql`, `oracle`, `spanner`, `sqlite`, `ydb` | to-catalog | both conversions carry it, and the comparison plans nothing when it changes |
| `synonym.Synonym.Name` | `oracle`, `sqlserver` | both | goschematodb drops it: the catalog is the same with and without it |
| `synonym.Synonym.Schema` | `oracle`, `sqlserver` | both | both conversions carry it, and the comparison plans nothing when it changes |
| `tsschema.DesiredContinuousAggregate.Body` | `cockroachdb`, `postgres`, `spanner`, `yugabytedb` | both | goschematodb drops it: the catalog is the same with and without it |

### Compared fields that do not reach a plan by design

61 exemptions, each with the reason it is right.

| Field | Dialects | Directions | Why |
| --- | --- | --- | --- |
| `pgtable.DesiredPartition.Type` | `clickhouse`, `mariadb`, `mysql`, `oracle`, `sqlite`, `sqlserver` | both | the target reports PostgreSQL partition declarations as omitted and does not manage their keys |
| `pgtable.DesiredPartition.Type` | `postgres`, `yugabytedb` | both | partition keys cannot change in place; the owner refuses a changed key or requests connected normalization |
| `pgtable.DesiredPartition.Parts` | `clickhouse`, `mariadb`, `mysql`, `oracle`, `sqlite`, `sqlserver` | both | the target reports PostgreSQL partition declarations as omitted and does not manage their keys |
| `pgtable.DesiredPartition.Parts` | `postgres`, `yugabytedb` | both | partition keys cannot change in place; the owner refuses a changed key or requests connected normalization |
| `pgtable.PartitionPart.Name` | `clickhouse`, `mariadb`, `mysql`, `oracle`, `sqlite`, `sqlserver` | both | the target reports PostgreSQL partition declarations as omitted and does not manage their keys |
| `pgtable.PartitionPart.Name` | `postgres`, `yugabytedb` | both | partition keys cannot change in place; the owner refuses a changed key or requests connected normalization |
| `pgtable.PartitionPart.Expr` | `mariadb`, `mysql`, `oracle`, `sqlite`, `sqlserver` | both | the target reports PostgreSQL partition declarations as omitted and does not manage their keys |
| `pgtable.PartitionPart.Expr` | `postgres`, `yugabytedb` | both | partition keys cannot change in place; the owner refuses a changed key or requests connected normalization |
| `pgtable.PartitionPart.Collation` | `postgres`, `yugabytedb` | both | partition keys cannot change in place; the owner refuses a changed key or requests connected normalization |
| `pgtable.PartitionPart.OperatorClass` | `postgres`, `yugabytedb` | both | partition keys cannot change in place; the owner refuses a changed key or requests connected normalization |
| `chschema.GranularitySetting.State` | `clickhouse` | both | a granularity's state changes only with its value: an explicit one carries a value and no other state may, so no single change of the state is valid. The value beside it is reached |
| `chschema.RoleSelection.All` | `clickhouse` | both | TO ALL changes only with its exceptions: exceptions stand only beside TO ALL, and the fixture that sets it names one. The selection's names and exceptions are reached |
| `chschema.Setting.State` | `clickhouse` | both | a setting's state changes only with its value: an explicit one carries a value and no other state may, so no single change of the state is valid. The value beside it is reached |
| `coverage.Object.Kind` | any | both | records what a description does not describe, which turns an absence on one side into no conclusion. A schema compared with its own conversion holds the same objects on both sides, so no change of it can plan something; migration/schemadiff's coverage tests compare it with a read that disagrees |
| `coverage.Object.Name` | any | both | records what a description does not describe, which turns an absence on one side into no conclusion. A schema compared with its own conversion holds the same objects on both sides, so no change of it can plan something; migration/schemadiff's coverage tests compare it with a read that disagrees |
| `coverage.Object.Provenance` | any | both | records what a description does not describe, which turns an absence on one side into no conclusion. A schema compared with its own conversion holds the same objects on both sides, so no change of it can plan something; migration/schemadiff's coverage tests compare it with a read that disagrees |
| `coverage.Object.Reason` | any | both | records what a description does not describe, which turns an absence on one side into no conclusion. A schema compared with its own conversion holds the same objects on both sides, so no change of it can plan something; migration/schemadiff's coverage tests compare it with a read that disagrees |
| `coverage.Set.Objects` | any | both | records what a description does not describe, which turns an absence on one side into no conclusion. A schema compared with its own conversion holds the same objects on both sides, so no change of it can plan something; migration/schemadiff's coverage tests compare it with a read that disagrees |
| `mssqlschema.Predicate.Type` | `sqlserver` | both | the fixture's other predicate is a BLOCK for every operation, and a second BLOCK beside it is refused, so no single change of the type is valid there. The predicate's other fields are reached |
| `pgextension.DesiredExtension.IfNotExists` | `postgres`, `yugabytedb` | both | IF NOT EXISTS says how a statement behaves when the object exists; no catalog records it |
| `schemamodel.Constraint.NotValid` | `cockroachdb`, `postgres`, `yugabytedb` | both | NOT VALID permits an unvalidated constraint and does not demand one: a validated one satisfies it, and no statement unvalidates a constraint. Without it, an unvalidated constraint plans VALIDATE CONSTRAINT |
| `pgpolicy.RoleSelector.Keyword` | `cockroachdb`, `postgres`, `yugabytedb` | both | a role keyword such as CURRENT_USER has no observation until a server resolves it, and the conversion refuses it; a connected comparison reads the server's resolution from pgpolicy.NormalizedPolicy.Roles |
| `pgtype.DesiredDomain.Check` | `postgres`, `yugabytedb` | both | an offline comparison leaves a domain's CHECK and DEFAULT create-only and compares them only once a server has normalized them |
| `pgtype.DesiredDomain.Default` | `postgres`, `yugabytedb` | both | an offline comparison leaves a domain's CHECK and DEFAULT create-only and compares them only once a server has normalized them |
| `pgtype.DesiredDomain.DefaultExpr` | `postgres`, `yugabytedb` | both | an offline comparison leaves a domain's CHECK and DEFAULT create-only and compares them only once a server has normalized them |
| `pgtype.DesiredRange.Cleared` | any | both | a cleared range attribute converts to the empty attribute an absent one converts to, so it differs only from a current range that holds the attribute; pgtypeprovider.TestCompareTypes_FindsChanges measures that |
| `oratype.DesiredDomain.Check` | `oracle` | both | Oracle stores its own spelling of a domain's CHECK and DEFAULT and no probe asks the server how it spells a declaration's, so a comparison leaves both create-only |
| `oratype.DesiredDomain.Default` | `oracle` | both | Oracle stores its own spelling of a domain's CHECK and DEFAULT and no probe asks the server how it spells a declaration's, so a comparison leaves both create-only |
| `oratype.DesiredDomain.DefaultExpr` | `oracle` | both | Oracle stores its own spelling of a domain's CHECK and DEFAULT and no probe asks the server how it spells a declaration's, so a comparison leaves both create-only |
| `schemamodel.Database.FeatureCoverage` | any | both | it is what a feature comparison may conclude from: without it the comparison refuses the objects as not inspected, which is the comparison reading it |
| `schemamodel.Database.NotDescribed` | any | both | records what a description does not describe, which turns an absence on one side into no conclusion. A schema compared with its own conversion holds the same objects on both sides, so no change of it can plan something; migration/schemadiff's coverage tests compare it with a read that disagrees |
| `schemamodel.Field.Primary` | `clickhouse` | both | every fixture's only key column is its primary one, and a ClickHouse MergeTree table without a sorting key is refused |
| `schemamodel.Function.Body` | `mariadb`, `mysql` | from-catalog | a routine that states no security mode is a definer routine here, a definer routine is replaced only beside a recorded definer and connected account, and a description converted from a catalog records neither, so the comparison refuses the replacement and names the routine |
| `schemamodel.Grant.Dialects` | `clickhouse`, `mariadb`, `mysql`, `oracle`, `postgres`, `spanner`, `sqlite`, `sqlserver` | both | removing the selection puts on the converted side a grant to a role the declaration does not manage on that target, and the comparison removes grants only from the roles it manages |
| `schemamodel.Grant.Dialects` | `cockroachdb`, `yugabytedb` | to-catalog | removing the selection puts on the converted side a grant to a role the declaration does not manage on that target, and the comparison removes grants only from the roles it manages |
| `schemamodel.Index.Operator` | `cockroachdb` | both | CockroachDB reports no operator class: on v26.3.2 pg_index.indclass is 0 for every key and pg_opclass is empty, also for an index built with gin_trgm_ops |
| `schemamodel.Index.StorageParams` | `cockroachdb` | both | CockroachDB accepts WITH (fillfactor = 70) on CREATE INDEX and stores nothing: pg_class.reloptions stays empty on v26.3.2 |
| `schemamodel.IndexPart.NullsOrder` | `cockroachdb` | both | CockroachDB has no NULLS FIRST or NULLS LAST in an index key (v26.3.2 answers `unimplemented: this syntax`) and orders NULLs by the key's direction, which the comparison reads |
| `schemamodel.IndexPart.Operator` | `cockroachdb` | both | CockroachDB reports no operator class: on v26.3.2 pg_index.indclass is 0 for every key and pg_opclass is empty, also for an index built with gin_trgm_ops |
| `schemamodel.Index.StorageParams` | `yugabytedb` | both | YugabyteDB 2026.1.2 refuses every index storage parameter Ptah writes: fillfactor and deduplicate_items are unrecognized, ybgin does not support fastupdate, and BRIN, which pages_per_range belongs to, is not supported |
| `schemamodel.Index.Operator` | `spanner` | both | Spanner refuses an operator class in CREATE INDEX ("Setting operator class is not supported", measured on the emulator behind PGAdapter v0.56.1), so no index it holds has one to compare |
| `schemamodel.Index.StorageParams` | `spanner` | both | Spanner refuses storage parameters in CREATE INDEX ("Index options are not supported", measured on the emulator behind PGAdapter v0.56.1), so no index it holds has one to compare |
| `schemamodel.Index.Type` | `spanner` | both | Spanner refuses an access method in CREATE INDEX ("Setting access method is not supported", measured on the emulator behind PGAdapter v0.56.1), so no index it holds has one to compare |
| `schemamodel.IndexPart.Operator` | `spanner` | both | Spanner refuses an operator class in CREATE INDEX ("Setting operator class is not supported", measured on the emulator behind PGAdapter v0.56.1), so no index it holds has one to compare |
| `schemamodel.RLSEnabledTable.Forced` | any | both | a shared row-level security declaration reaches no catalog: each target converts it to its owner's model or skips it, so only the dialect-free comparison of two declarations reads it |
| `schemamodel.RLSPolicy.Comment` | any | both | a shared row-level security declaration reaches no catalog: each target converts it to its owner's model or skips it, so only the dialect-free comparison of two declarations reads it |
| `schemamodel.Role.Dialects` | `clickhouse`, `cockroachdb`, `mariadb`, `mysql`, `oracle`, `postgres`, `spanner`, `sqlite`, `sqlserver`, `ydb`, `yugabytedb` | both | removing the selection either changes nothing, on a target the role is selected for, or puts on the converted side a role the declaration does not name, and the comparison never removes such a role (compare.TestRolesComparison) |
| `schemamodel.Role.Password` | `cockroachdb`, `postgres`, `yugabytedb` | from-catalog | a catalog holds no readable password, so a declaration converted back from one has none |
| `schemamodel.Schema.Charset` | `mariadb`, `mysql` | both | a MySQL-family schema is a database, and only a whole-server comparison compares databases (stokaro/ptah#3789); this comparison reads one database |
| `schemamodel.Schema.Collate` | `mariadb`, `mysql` | both | a MySQL-family schema is a database, and only a whole-server comparison compares databases (stokaro/ptah#3789); this comparison reads one database |
| `schemamodel.Sequence.IfNotExists` | `cockroachdb`, `mariadb`, `oracle`, `postgres`, `sqlserver`, `yugabytedb` | both | IF NOT EXISTS says how a statement behaves when the object exists; no catalog records it |
| `schemamodel.Table.CustomSQL` | `clickhouse`, `cockroachdb`, `mariadb`, `mysql`, `oracle`, `postgres`, `spanner`, `sqlite`, `sqlserver`, `ydb`, `yugabytedb` | both | statements the author appends verbatim; no catalog describes them, so a comparison has nothing to hold them against |
| `sqlitetable.DesiredVirtual.Virtual` | `sqlite` | both | the comparison refuses any change of a SQLite virtual table, because rebuilding it destroys the module's storage (stokaro/ptah#1028); the refusal names the table, which is the comparison reading it |
| `sqlitetable.Virtual.Arguments` | `sqlite` | both | the comparison refuses any change of a SQLite virtual table, because rebuilding it destroys the module's storage (stokaro/ptah#1028); the refusal names the table, which is the comparison reading it |
| `sqlitetable.Virtual.Module` | `sqlite` | both | the comparison refuses any change of a SQLite virtual table, because rebuilding it destroys the module's storage (stokaro/ptah#1028); the refusal names the table, which is the comparison reading it |
| `tsschema.DesiredHypertable.IfNotExists` | `cockroachdb`, `postgres`, `spanner`, `yugabytedb` | both | IF NOT EXISTS says how a statement behaves when the object exists; no catalog records it |
| `ydbreplication.ReplicationSpec.ConsistencyLevel` | `ydb` | both | the fixture declares a commit interval, which belongs to the global level alone, so no other level is valid beside it. The commit interval is reached |
| `ydbschema.ChangefeedSpec.Format` | `ydb` | both | the fixture's changefeed asks for virtual timestamps, which YDB writes only in JSON, so no other format is valid beside them. The timestamps are reached |
| `ydbschema.ChangefeedSpec.Name` | `ydb` | both | repeats the changefeed's identity, which its subject key holds; a definition that disagrees with its key is refused, and a changefeed under another key is another object |
| `ydbsecret.Desired.ValueEnv` | `ydb` | both | names the environment variable a secret's value is read from when it is applied; YDB never shows a secret's value |
| `ydbstreaming.Desired.AllowStateReset` | `ydb` | both | permits a plan to reset a streaming query's state when its body changes; the permission is not stored in the database |

### Compared fields the round trip cannot ask about

20, besides the ones only a recorded loss hides.

| Field | Why |
| --- | --- |
| `pgtable.DesiredPartition.Normalized` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `pgtable.DesiredExclusion.Normalized` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `pgtable.NormalizedExclusion.SourceKey` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `pgtable.NormalizedExclusion.Elements` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `pgtable.NormalizedExclusion.WhereCondition` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `pgtable.NormalizedPartition.SourceKey` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `pgtable.NormalizedPartition.Key` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `mssqlschema.DesiredSecurityPolicy.Normalized` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `pgtype.DesiredDomain.Normalized` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `pgtype.NormalizedDomain.Check` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `pgtype.NormalizedDomain.Default` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `schemamodel.Database.Facets` | declared only by the census's unknown-facet fixture, which every target refuses |
| `schemamodel.Enum.Facets` | declared only by the census's unknown-facet fixture, which every target refuses |
| `schemamodel.Field.UniqueExpr` | every target refuses it: uniqueness over an expression is not implemented |
| `schemamodel.Schema.Facets` | declared only by the census's unknown-facet fixture, which every target refuses |
| `schemamodel.Sequence.Facets` | declared only by the census's unknown-facet fixture, which every target refuses |
| `tsschema.DesiredContinuousAggregate.Normalized` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `tsschema.NormalizedBody.Body` | no census fixture declares a server's spelling; a connected comparison attaches one |
| `ydbschema.ChangefeedSpec.Disabled` | every target refuses it: YDB has no statement that disables a changefeed |
| `ydbschema.ColumnFamily.KeepInMemory` | only a read sets it; no declaration carries it |

### Every field

| Field | Disposition | Why it is not rendered |
| --- | --- | --- |
| `chschema.DesiredIndex.Granularity` | `ddl` | — |
| `chschema.DesiredIndex.IndexType` | `ddl` | — |
| `chschema.DesiredRefresh.Schedule` | `ddl` | — |
| `chschema.DesiredRowPolicy.Composition` | `ddl` | — |
| `chschema.DesiredRowPolicy.Filter` | `ddl` | — |
| `chschema.DesiredRowPolicy.NormalizedFilter` | `comparison` | a connected server's spelling of the declared filter, attached before a live comparison; a statement writes the filter as declared |
| `chschema.DesiredRowPolicy.Roles` | `ddl` | — |
| `chschema.DesiredRowPolicy.StructName` | `source` | the Go struct the declaration was read from; the policy's database, table and name are its identity |
| `chschema.DesiredTable.Engine` | `ddl` | — |
| `chschema.DesiredTable.OrderBy` | `ddl` | — |
| `chschema.DesiredTable.PartitionBy` | `ddl` | — |
| `chschema.DesiredTable.PrimaryKey` | `ddl` | — |
| `chschema.DesiredTable.SampleBy` | `ddl` | — |
| `chschema.DesiredTable.Settings` | `ddl` | — |
| `chschema.DesiredTable.TTL` | `ddl` | — |
| `chschema.GranularitySetting.State` | `ddl` | — |
| `chschema.GranularitySetting.Value` | `ddl` | — |
| `chschema.RoleSelection.All` | `ddl` | — |
| `chschema.RoleSelection.Except` | `ddl` | — |
| `chschema.RoleSelection.Names` | `ddl` | — |
| `chschema.Schedule.Append` | `ddl` | — |
| `chschema.Schedule.DependsOn` | `ddl` | — |
| `chschema.Schedule.Interval` | `ddl` | — |
| `chschema.Schedule.Mode` | `ddl` | — |
| `chschema.Schedule.Offset` | `ddl` | — |
| `chschema.Schedule.Randomize` | `ddl` | — |
| `chschema.Setting.State` | `ddl` | — |
| `chschema.Setting.Value` | `ddl` | — |
| `coverage.Object.Kind` | `comparison` | which kind the undescribed object is |
| `coverage.Object.Name` | `comparison` | which object was not described |
| `coverage.Object.Provenance` | `comparison` | how Ptah learned the object was not described |
| `coverage.Object.Reason` | `comparison` | why it was not described |
| `coverage.Set.Objects` | `comparison` | the per-object half of that record |
| `crdbschema.DesiredRowTTL.Policy` | `ddl` | — |
| `crdbschema.Policy.DeleteBatchSize` | `ddl` | — |
| `crdbschema.Policy.DeleteRateLimit` | `ddl` | — |
| `crdbschema.Policy.DisableChangefeedReplication` | `ddl` | — |
| `crdbschema.Policy.ExpirationExpression` | `ddl` | — |
| `crdbschema.Policy.ExpireAfter` | `ddl` | — |
| `crdbschema.Policy.JobCron` | `ddl` | — |
| `crdbschema.Policy.LabelMetrics` | `ddl` | — |
| `crdbschema.Policy.Pause` | `ddl` | — |
| `crdbschema.Policy.RowStatsPollInterval` | `ddl` | — |
| `crdbschema.Policy.SelectBatchSize` | `ddl` | — |
| `crdbschema.Policy.SelectRateLimit` | `ddl` | — |
| `mssqlproperty.DesiredProperty.Comment` | `ddl` | — |
| `mssqlproperty.DesiredProperty.Property` | `ddl` | — |
| `mssqlproperty.DesiredProperty.StructName` | `source` | the Go struct the declaration was read from; the property's address and name are its identity |
| `mssqlproperty.Property.Column` | `ddl` | — |
| `mssqlproperty.Property.Name` | `ddl` | — |
| `mssqlproperty.Property.Schema` | `ddl` | — |
| `mssqlproperty.Property.Table` | `ddl` | — |
| `mssqlproperty.Property.Value` | `ddl` | — |
| `mssqlschema.DesiredRoutineOptions.Principal` | `ddl` | — |
| `mssqlschema.DesiredRoutineOptions.Recompile` | `ddl` | — |
| `mssqlschema.DesiredRoutineOptions.ReturnsNullOnNullInput` | `ddl` | — |
| `mssqlschema.DesiredRoutineOptions.SchemaBinding` | `ddl` | — |
| `mssqlschema.DesiredSecurityPolicy.Enabled` | `ddl` | — |
| `mssqlschema.DesiredSecurityPolicy.Normalized` | `comparison` | a connected server's spelling of the declared predicates, attached before a live comparison; a statement writes the predicates as declared |
| `mssqlschema.DesiredSecurityPolicy.NotForReplication` | `ddl` | — |
| `mssqlschema.DesiredSecurityPolicy.Predicates` | `ddl` | — |
| `mssqlschema.DesiredSecurityPolicy.SchemaBinding` | `ddl` | — |
| `mssqlschema.DesiredSecurityPolicy.StructName` | `source` | the Go struct the declaration was read from; the policy's schema and name are its identity |
| `mssqlschema.ObjectName.Name` | `ddl` | — |
| `mssqlschema.ObjectName.Schema` | `ddl` | — |
| `mssqlschema.Predicate.Arguments` | `ddl` | — |
| `mssqlschema.Predicate.Function` | `ddl` | — |
| `mssqlschema.Predicate.Operation` | `ddl` | — |
| `mssqlschema.Predicate.Table` | `ddl` | — |
| `mssqlschema.Predicate.Type` | `ddl` | — |
| `mysqlschema.DesiredColumnSettings.Charset` | `ddl` | — |
| `mysqlschema.DesiredColumnSettings.OnUpdate` | `ddl` | — |
| `mysqlschema.DesiredIndex.Parser` | `ddl` | — |
| `mysqlschema.DesiredIndexBlockSize.KeyBlockSize` | `ddl` | — |
| `mysqlschema.DesiredTable.AutoIncrement` | `ddl` | — |
| `mysqlschema.DesiredTable.Charset` | `ddl` | — |
| `mysqlschema.DesiredTable.Engine` | `ddl` | — |
| `oratype.Attribute.Name` | `ddl` | — |
| `oratype.Attribute.Type` | `ddl` | — |
| `oratype.DesiredDomain.BaseType` | `ddl` | — |
| `oratype.DesiredDomain.Check` | `ddl` | — |
| `oratype.DesiredDomain.Comment` | `ddl` | — |
| `oratype.DesiredDomain.Default` | `ddl` | — |
| `oratype.DesiredDomain.DefaultExpr` | `ddl` | — |
| `oratype.DesiredDomain.NotNull` | `ddl` | — |
| `oratype.DesiredDomain.StructName` | `source` | the Go struct the declaration was read from; the type's schema and name are its identity |
| `oratype.DesiredObjectType.Attributes` | `ddl` | — |
| `oratype.DesiredObjectType.Comment` | `ddl` | — |
| `oratype.DesiredObjectType.StructName` | `source` | the Go struct the declaration was read from; the type's schema and name are its identity |
| `pgdefaultprivilege.Desired.Comment` | `ddl` | — |
| `pgdefaultprivilege.Desired.Privileges` | `ddl` | — |
| `pgdefaultprivilege.Desired.Revoked` | `ddl` | — |
| `pgdefaultprivilege.Grant.Privilege` | `ddl` | — |
| `pgdefaultprivilege.Grant.WithOption` | `ddl` | — |
| `pgextension.DesiredExtension.Comment` | `ddl` | — |
| `pgextension.DesiredExtension.IfNotExists` | `ddl` | — |
| `pgextension.DesiredExtension.Provides` | `export` | what a read reported the extension supplies; the Atlas-compatible HCL export keeps a block the document names through one, and nothing renders or compares it |
| `pgextension.DesiredExtension.Schema` | `ddl` | — |
| `pgextension.DesiredExtension.Version` | `ddl` | — |
| `pgpolicy.DesiredPolicy.Command` | `ddl` | — |
| `pgpolicy.DesiredPolicy.Comment` | `ddl` | — |
| `pgpolicy.DesiredPolicy.Composition` | `ddl` | — |
| `pgpolicy.DesiredPolicy.Normalized` | `comparison` | a connected server's spelling of the declared roles and clauses, attached before a live comparison; a statement writes the declaration |
| `pgpolicy.DesiredPolicy.Roles` | `ddl` | — |
| `pgpolicy.DesiredPolicy.StructName` | `source` | the Go struct the declaration was read from; the policy's schema, table and name are its identity |
| `pgpolicy.DesiredPolicy.Using` | `ddl` | — |
| `pgpolicy.DesiredPolicy.WithCheck` | `ddl` | — |
| `pgpolicy.DesiredTableState.Comment` | `ddl` | — |
| `pgpolicy.DesiredTableState.Enabled` | `ddl` | — |
| `pgpolicy.DesiredTableState.Forced` | `ddl` | — |
| `pgpolicy.DesiredTableState.StructName` | `source` | the Go struct the declaration was read from; the switches belong to the table that carries them |
| `pgpolicy.NormalizedPolicy.Roles` | `comparison` | the server's resolution of the declared roles, compared with the catalog's and never rendered |
| `pgpolicy.NormalizedPolicy.Using` | `comparison` | the server's spelling of a declared USING expression, compared with the catalog's and never rendered |
| `pgpolicy.NormalizedPolicy.WithCheck` | `comparison` | the server's spelling of a declared WITH CHECK expression, compared with the catalog's and never rendered |
| `pgpolicy.RoleSelector.Keyword` | `ddl` | — |
| `pgpolicy.RoleSelector.Name` | `ddl` | — |
| `pgroutine.DesiredProperties.Leakproof` | `ddl` | — |
| `pgroutine.DesiredProperties.Parallel` | `ddl` | — |
| `pgroutine.DesiredProperties.Strict` | `ddl` | — |
| `pgtable.DesiredDeleteColumns.Columns` | `ddl` | — |
| `pgtable.DesiredExclusion.Exclusion` | `ddl` | — |
| `pgtable.DesiredExclusion.Normalized` | `comparison` | the connected server's expression spelling; source encoders preserve the declaration |
| `pgtable.DesiredExclusion.StructName` | `source` | the Go struct the declaration belongs to; its table and name are the object identity |
| `pgtable.DesiredPartition.Normalized` | `comparison` | the connected server's key for the declaration; source encoders preserve the declared spelling |
| `pgtable.DesiredPartition.Parts` | `ddl` | — |
| `pgtable.DesiredPartition.Type` | `ddl` | — |
| `pgtable.DesiredPersistence.Unlogged` | `ddl` | — |
| `pgtable.Exclusion.Comment` | `ddl` | — |
| `pgtable.Exclusion.Deferrable` | `ddl` | — |
| `pgtable.Exclusion.Dependencies` | `export` | catalog dependencies preserve required extensions during export even when the SQL definition omits their operator classes |
| `pgtable.Exclusion.Elements` | `ddl` | — |
| `pgtable.Exclusion.Initially` | `ddl` | — |
| `pgtable.Exclusion.UsingMethod` | `ddl` | — |
| `pgtable.Exclusion.WhereCondition` | `ddl` | — |
| `pgtable.NormalizedExclusion.Elements` | `comparison` | the server's expression spelling, compared with the catalog and never rendered |
| `pgtable.NormalizedExclusion.SourceKey` | `comparison` | binds the normalized expressions to the declaration |
| `pgtable.NormalizedExclusion.WhereCondition` | `comparison` | the server's predicate spelling, compared with the catalog and never rendered |
| `pgtable.NormalizedPartition.Key` | `comparison` | the server's normalized partition key, compared with the catalog and never rendered |
| `pgtable.NormalizedPartition.SourceKey` | `comparison` | binds the normalized answer to its declaration |
| `pgtable.PartitionPart.Collation` | `ddl` | — |
| `pgtable.PartitionPart.Expr` | `ddl` | — |
| `pgtable.PartitionPart.Name` | `ddl` | — |
| `pgtable.PartitionPart.OperatorClass` | `ddl` | — |
| `pgtype.Attribute.Name` | `ddl` | — |
| `pgtype.Attribute.Type` | `ddl` | — |
| `pgtype.DesiredComposite.Attributes` | `ddl` | — |
| `pgtype.DesiredComposite.Comment` | `ddl` | — |
| `pgtype.DesiredComposite.StructName` | `source` | the Go struct the declaration was read from; the type's schema and name are its identity |
| `pgtype.DesiredDomain.BaseType` | `ddl` | — |
| `pgtype.DesiredDomain.Check` | `ddl` | — |
| `pgtype.DesiredDomain.Comment` | `ddl` | — |
| `pgtype.DesiredDomain.Default` | `ddl` | — |
| `pgtype.DesiredDomain.DefaultExpr` | `ddl` | — |
| `pgtype.DesiredDomain.Normalized` | `comparison` | a connected server's spelling of the declared CHECK and default, attached before a live comparison; a statement writes the declaration |
| `pgtype.DesiredDomain.NotNull` | `ddl` | — |
| `pgtype.DesiredDomain.StructName` | `source` | the Go struct the declaration was read from; the type's schema and name are its identity |
| `pgtype.DesiredRange.Canonical` | `ddl` | — |
| `pgtype.DesiredRange.Cleared` | `comparison` | names a range attribute the declaration writes empty, which a comparison tells apart from one it leaves out; a statement writes neither (stokaro/ptah#2223) |
| `pgtype.DesiredRange.Collation` | `ddl` | — |
| `pgtype.DesiredRange.Comment` | `ddl` | — |
| `pgtype.DesiredRange.StructName` | `source` | the Go struct the declaration was read from; the type's schema and name are its identity |
| `pgtype.DesiredRange.Subtype` | `ddl` | — |
| `pgtype.DesiredRange.SubtypeDiff` | `ddl` | — |
| `pgtype.DesiredRange.SubtypeOpClass` | `ddl` | — |
| `pgtype.NormalizedDomain.Check` | `comparison` | the server's spelling of a declared domain CHECK, compared with the catalog's and never rendered |
| `pgtype.NormalizedDomain.Default` | `comparison` | the server's spelling of a declared domain default, compared with the catalog's and never rendered |
| `schemamodel.Constraint.CheckExpression` | `ddl` | — |
| `schemamodel.Constraint.Columns` | `ddl` | — |
| `schemamodel.Constraint.Comment` | `ddl` | — |
| `schemamodel.Constraint.Deferrable` | `ddl` | — |
| `schemamodel.Constraint.Facets` | `ddl` | — |
| `schemamodel.Constraint.ForeignColumn` | `ddl` | — |
| `schemamodel.Constraint.ForeignColumns` | `ddl` | — |
| `schemamodel.Constraint.ForeignTable` | `ddl` | — |
| `schemamodel.Constraint.IncludeColumns` | `ddl` | — |
| `schemamodel.Constraint.Initially` | `ddl` | — |
| `schemamodel.Constraint.Match` | `ddl` | — |
| `schemamodel.Constraint.Name` | `ddl` | — |
| `schemamodel.Constraint.NotEnforced` | `ddl` | — |
| `schemamodel.Constraint.NotValid` | `ddl` | — |
| `schemamodel.Constraint.NullsDistinct` | `ddl` | — |
| `schemamodel.Constraint.OnDelete` | `ddl` | — |
| `schemamodel.Constraint.OnUpdate` | `ddl` | — |
| `schemamodel.Constraint.Requires` | `planning` | the objects, such as extensions, that must exist before this constraint can be created; it orders the statements and appears in none of them |
| `schemamodel.Constraint.StructName` | `ddl` | — |
| `schemamodel.Constraint.Table` | `ddl` | — |
| `schemamodel.Constraint.Type` | `ddl` | — |
| `schemamodel.Constraint.UsingMethod` | `ddl` | — |
| `schemamodel.Database.Constraints` | `ddl` | — |
| `schemamodel.Database.Dependencies` | `derived` | table creation order, derived by Finalize from the declared foreign keys |
| `schemamodel.Database.EmbeddedFields` | `ddl` | — |
| `schemamodel.Database.EmbeddedSources` | `ddl` | — |
| `schemamodel.Database.Enums` | `ddl` | — |
| `schemamodel.Database.Facets` | `ddl` | — |
| `schemamodel.Database.FeatureCoverage` | `comparison` | records source knowledge for exact feature models and subjects; limits which state can be compared or reconstructed |
| `schemamodel.Database.FeatureObjects` | `ddl` | — |
| `schemamodel.Database.Fields` | `ddl` | — |
| `schemamodel.Database.FunctionDependencies` | `derived` | function creation order, derived by Finalize from the declared bodies |
| `schemamodel.Database.Functions` | `ddl` | — |
| `schemamodel.Database.Grants` | `ddl` | — |
| `schemamodel.Database.Indexes` | `ddl` | — |
| `schemamodel.Database.ManagedData` | `data` | reference and seed rows; `ptah seed` writes them and `ptah schema render` does not |
| `schemamodel.Database.MaterializedViews` | `ddl` | — |
| `schemamodel.Database.NotDescribed` | `comparison` | what the description does not claim to describe; it decides what a diff may conclude about an object nobody looked at, and reaches no statement |
| `schemamodel.Database.RLSEnabledTables` | `ddl` | — |
| `schemamodel.Database.RLSPolicies` | `ddl` | — |
| `schemamodel.Database.RevokedGrants` | `ddl` | — |
| `schemamodel.Database.Roles` | `ddl` | — |
| `schemamodel.Database.Schemas` | `ddl` | — |
| `schemamodel.Database.SelfReferencingForeignKeys` | `derived` | derived by Finalize from the declared foreign keys, so the planner can create the table before the reference to itself |
| `schemamodel.Database.Sequences` | `ddl` | — |
| `schemamodel.Database.Tables` | `ddl` | — |
| `schemamodel.Database.Triggers` | `ddl` | — |
| `schemamodel.Database.Views` | `ddl` | — |
| `schemamodel.EmbeddedField.Comment` | `ddl` | — |
| `schemamodel.EmbeddedField.EmbeddedTypeName` | `ddl` | — |
| `schemamodel.EmbeddedField.Field` | `ddl` | — |
| `schemamodel.EmbeddedField.Mode` | `ddl` | — |
| `schemamodel.EmbeddedField.Name` | `ddl` | — |
| `schemamodel.EmbeddedField.Nullable` | `ddl` | — |
| `schemamodel.EmbeddedField.OnDelete` | `ddl` | — |
| `schemamodel.EmbeddedField.OnUpdate` | `ddl` | — |
| `schemamodel.EmbeddedField.Overrides` | `ddl` | — |
| `schemamodel.EmbeddedField.Prefix` | `ddl` | — |
| `schemamodel.EmbeddedField.Ref` | `ddl` | — |
| `schemamodel.EmbeddedField.StructName` | `ddl` | — |
| `schemamodel.EmbeddedField.Type` | `ddl` | — |
| `schemamodel.EmbeddedSources.Definitions` | `derived` | the embedded declarations retained so materialization can run again after a merge; the columns they produce are what reaches DDL |
| `schemamodel.EmbeddedSources.Fields` | `ddl` | — |
| `schemamodel.Enum.Comment` | `ddl` | — |
| `schemamodel.Enum.Facets` | `ddl` | — |
| `schemamodel.Enum.Name` | `ddl` | — |
| `schemamodel.Enum.Schema` | `ddl` | — |
| `schemamodel.Enum.Values` | `ddl` | — |
| `schemamodel.Field.APIExpose` | `export` | whether the column reaches an exported API contract, and in which direction; Ptah emits no runtime that could enforce it |
| `schemamodel.Field.APIName` | `export` | the name an exported API document carries when it differs from the database name |
| `schemamodel.Field.APINames` | `export` | the per-format names an exported API document carries, overriding the general one |
| `schemamodel.Field.APIType` | `export` | the type an exported document should project the column as, which changes no stored value |
| `schemamodel.Field.AutoInc` | `ddl` | — |
| `schemamodel.Field.Check` | `ddl` | — |
| `schemamodel.Field.CheckName` | `ddl` | — |
| `schemamodel.Field.CheckNotEnforced` | `ddl` | — |
| `schemamodel.Field.Collate` | `ddl` | — |
| `schemamodel.Field.Comment` | `ddl` | — |
| `schemamodel.Field.Default` | `ddl` | — |
| `schemamodel.Field.DefaultExpr` | `ddl` | — |
| `schemamodel.Field.DefaultSet` | `ddl` | — |
| `schemamodel.Field.Deferrable` | `ddl` | — |
| `schemamodel.Field.Enum` | `ddl` | — |
| `schemamodel.Field.Facets` | `ddl` | — |
| `schemamodel.Field.FieldName` | `source` | the Go struct field the column was read from; the column's own name is its identity. The only render that moves under its ablation is the PostgreSQL-family renderer walking table options in map order |
| `schemamodel.Field.Foreign` | `ddl` | — |
| `schemamodel.Field.ForeignKeyMatch` | `ddl` | — |
| `schemamodel.Field.ForeignKeyName` | `ddl` | — |
| `schemamodel.Field.ForeignKeyNotEnforced` | `ddl` | — |
| `schemamodel.Field.GeneratedExpression` | `ddl` | — |
| `schemamodel.Field.GeneratedFromEmbedded` | `derived` | marks a column Finalize materialized from an embedded declaration, so a later finalization can rebuild it rather than duplicate it |
| `schemamodel.Field.GeneratedKind` | `ddl` | — |
| `schemamodel.Field.IdentityGeneration` | `ddl` | — |
| `schemamodel.Field.IdentityIncrement` | `ddl` | — |
| `schemamodel.Field.IdentityOptions` | `ddl` | — |
| `schemamodel.Field.IdentityStart` | `ddl` | — |
| `schemamodel.Field.Initially` | `ddl` | — |
| `schemamodel.Field.Name` | `ddl` | — |
| `schemamodel.Field.NotNullConstraintName` | `ddl` | — |
| `schemamodel.Field.Nullable` | `ddl` | — |
| `schemamodel.Field.OnDelete` | `ddl` | — |
| `schemamodel.Field.OnUpdate` | `ddl` | — |
| `schemamodel.Field.Overrides` | `ddl` | — |
| `schemamodel.Field.Primary` | `ddl` | — |
| `schemamodel.Field.StructName` | `ddl` | — |
| `schemamodel.Field.Type` | `ddl` | — |
| `schemamodel.Field.TypeIsDeclaredText` | `ddl` | — |
| `schemamodel.Field.TypeRawSQL` | `ddl` | — |
| `schemamodel.Field.Unique` | `ddl` | — |
| `schemamodel.Field.UniqueExpr` | `ddl` | — |
| `schemamodel.Function.Body` | `ddl` | — |
| `schemamodel.Function.Comment` | `ddl` | — |
| `schemamodel.Function.Dialects` | `ddl` | — |
| `schemamodel.Function.Facets` | `ddl` | — |
| `schemamodel.Function.Kind` | `ddl` | — |
| `schemamodel.Function.Language` | `ddl` | — |
| `schemamodel.Function.Name` | `ddl` | — |
| `schemamodel.Function.Parameters` | `ddl` | — |
| `schemamodel.Function.Returns` | `ddl` | — |
| `schemamodel.Function.Security` | `ddl` | — |
| `schemamodel.Function.Settings` | `ddl` | — |
| `schemamodel.Function.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Function.Volatility` | `ddl` | — |
| `schemamodel.Grant.Columns` | `ddl` | — |
| `schemamodel.Grant.Comment` | `ddl` | — |
| `schemamodel.Grant.Dialects` | `ddl` | — |
| `schemamodel.Grant.GrantedBy` | `export` | the grantor a catalog read observed, carried so that a generated document can report it cannot represent one; PostgreSQL accepts GRANTED BY only for the role that IS the current user, so rendering the observed grantor would fail on every apply by another role |
| `schemamodel.Grant.ObjectKind` | `ddl` | — |
| `schemamodel.Grant.OnDatabase` | `ddl` | — |
| `schemamodel.Grant.OnObject` | `ddl` | — |
| `schemamodel.Grant.OnRoutine` | `ddl` | — |
| `schemamodel.Grant.OnSchema` | `ddl` | — |
| `schemamodel.Grant.OnSequence` | `ddl` | — |
| `schemamodel.Grant.OnTable` | `ddl` | — |
| `schemamodel.Grant.Privileges` | `ddl` | — |
| `schemamodel.Grant.Role` | `ddl` | — |
| `schemamodel.Grant.RoutineArguments` | `ddl` | — |
| `schemamodel.Grant.RoutineKind` | `ddl` | — |
| `schemamodel.Grant.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Grant.WithOption` | `ddl` | — |
| `schemamodel.Index.Comment` | `ddl` | — |
| `schemamodel.Index.Concurrently` | `planning` | asks that the index be BUILT without locking when added to a live table; internal/concurrentindex owns that decision, and only a plan carries it into DDL |
| `schemamodel.Index.Condition` | `ddl` | — |
| `schemamodel.Index.Facets` | `ddl` | — |
| `schemamodel.Index.Fields` | `ddl` | — |
| `schemamodel.Index.IncludeColumns` | `ddl` | — |
| `schemamodel.Index.Invisible` | `ddl` | — |
| `schemamodel.Index.Name` | `ddl` | — |
| `schemamodel.Index.NullsDistinct` | `ddl` | — |
| `schemamodel.Index.Operator` | `ddl` | — |
| `schemamodel.Index.Overrides` | `ddl` | — |
| `schemamodel.Index.Parts` | `ddl` | — |
| `schemamodel.Index.Requires` | `planning` | the same ordering fact for an index |
| `schemamodel.Index.StorageParams` | `ddl` | — |
| `schemamodel.Index.StructName` | `ddl` | — |
| `schemamodel.Index.TableName` | `ddl` | — |
| `schemamodel.Index.Type` | `ddl` | — |
| `schemamodel.Index.Unique` | `ddl` | — |
| `schemamodel.IndexPart.Desc` | `ddl` | — |
| `schemamodel.IndexPart.Expr` | `ddl` | — |
| `schemamodel.IndexPart.Name` | `ddl` | — |
| `schemamodel.IndexPart.NullsOrder` | `ddl` | — |
| `schemamodel.IndexPart.Operator` | `ddl` | — |
| `schemamodel.IndexPart.Prefix` | `ddl` | — |
| `schemamodel.ManagedData.File` | `data` | part of the reference-row declaration; `ptah seed` reads it and no renderer does |
| `schemamodel.ManagedData.Keys` | `data` | part of the reference-row declaration; `ptah seed` reads it and no renderer does |
| `schemamodel.ManagedData.Rows` | `data` | the declared rows themselves, read from File; the schema artifact carries these because it cannot carry the working copy File names |
| `schemamodel.ManagedData.Schema` | `data` | part of the reference-row declaration; `ptah seed` reads it and no renderer does |
| `schemamodel.ManagedData.SourceDir` | `data` | part of the reference-row declaration; `ptah seed` reads it and no renderer does |
| `schemamodel.ManagedData.StructName` | `data` | part of the reference-row declaration; `ptah seed` reads it and no renderer does |
| `schemamodel.ManagedData.Table` | `data` | part of the reference-row declaration; `ptah seed` reads it and no renderer does |
| `schemamodel.ManagedValue.Null` | `data` | a declared cell that is null, which is not the same as a column the row never names |
| `schemamodel.ManagedValue.Tag` | `data` | the YAML tag a declared cell resolved to; it separates 007 from "007" in one column |
| `schemamodel.ManagedValue.Text` | `data` | the exact text that declared a cell, kept because resolving it to a Go value loses leading zeros, decimal scale and date spelling |
| `schemamodel.MaterializedView.Body` | `ddl` | — |
| `schemamodel.MaterializedView.Comment` | `ddl` | — |
| `schemamodel.MaterializedView.DependsOn` | `planning` | the same ordering edge, on a materialized view |
| `schemamodel.MaterializedView.Dialects` | `ddl` | — |
| `schemamodel.MaterializedView.Facets` | `ddl` | — |
| `schemamodel.MaterializedView.Name` | `ddl` | — |
| `schemamodel.MaterializedView.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.PrimaryKeyPart.Desc` | `ddl` | — |
| `schemamodel.PrimaryKeyPart.Name` | `ddl` | — |
| `schemamodel.PrimaryKeyPart.Prefix` | `ddl` | — |
| `schemamodel.RLSEnabledTable.Comment` | `export` | the comment a shared enablement carries for the HCL and Go writers; no target writes a shared enablement |
| `schemamodel.RLSEnabledTable.Dialects` | `ddl` | — |
| `schemamodel.RLSEnabledTable.Forced` | `comparison` | FORCE is PostgreSQL's, whose switches the row-security owner holds; the dialect-neutral comparison of two shared declarations still compares it |
| `schemamodel.RLSEnabledTable.StructName` | `ddl` | — |
| `schemamodel.RLSEnabledTable.Table` | `ddl` | — |
| `schemamodel.RLSPolicy.Comment` | `comparison` | no target renders a shared row-level security policy: PostgreSQL, SQL Server and ClickHouse read theirs into their owners' models, and every other target names one and skips it; the dialect-neutral comparison of two shared declarations still compares it |
| `schemamodel.RLSPolicy.Dialects` | `ddl` | — |
| `schemamodel.RLSPolicy.Name` | `ddl` | — |
| `schemamodel.RLSPolicy.PolicyFor` | `comparison` | no target renders a shared row-level security policy: PostgreSQL, SQL Server and ClickHouse read theirs into their owners' models, and every other target names one and skips it; the dialect-neutral comparison of two shared declarations still compares it |
| `schemamodel.RLSPolicy.Restrictive` | `comparison` | no target renders a shared policy; the Go and HCL writers carry AS, and the dialect-neutral comparison of two shared declarations still compares it |
| `schemamodel.RLSPolicy.StructName` | `ddl` | — |
| `schemamodel.RLSPolicy.Table` | `ddl` | — |
| `schemamodel.RLSPolicy.ToRoles` | `comparison` | no target renders a shared row-level security policy: PostgreSQL, SQL Server and ClickHouse read theirs into their owners' models, and every other target names one and skips it; the dialect-neutral comparison of two shared declarations still compares it |
| `schemamodel.RLSPolicy.UsingExpression` | `comparison` | no target renders a shared row-level security policy: PostgreSQL, SQL Server and ClickHouse read theirs into their owners' models, and every other target names one and skips it; the dialect-neutral comparison of two shared declarations still compares it |
| `schemamodel.RLSPolicy.WithCheckExpression` | `comparison` | no target renders a shared row-level security policy: PostgreSQL, SQL Server and ClickHouse read theirs into their owners' models, and every other target names one and skips it; the dialect-neutral comparison of two shared declarations still compares it |
| `schemamodel.Role.Comment` | `ddl` | — |
| `schemamodel.Role.CreateDB` | `ddl` | — |
| `schemamodel.Role.CreateRole` | `ddl` | — |
| `schemamodel.Role.Dialects` | `ddl` | — |
| `schemamodel.Role.Facets` | `ddl` | — |
| `schemamodel.Role.Group` | `ddl` | — |
| `schemamodel.Role.Inherit` | `ddl` | — |
| `schemamodel.Role.Login` | `ddl` | — |
| `schemamodel.Role.MemberOf` | `ddl` | — |
| `schemamodel.Role.Name` | `ddl` | — |
| `schemamodel.Role.Password` | `ddl` | — |
| `schemamodel.Role.Replication` | `ddl` | — |
| `schemamodel.Role.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Role.Superuser` | `ddl` | — |
| `schemamodel.Schema.Charset` | `ddl` | — |
| `schemamodel.Schema.Collate` | `ddl` | — |
| `schemamodel.Schema.Comment` | `ddl` | — |
| `schemamodel.Schema.Facets` | `ddl` | — |
| `schemamodel.Schema.Name` | `ddl` | — |
| `schemamodel.SelfReferencingFK.FieldName` | `derived` | part of that derived record |
| `schemamodel.SelfReferencingFK.Foreign` | `derived` | part of that derived record |
| `schemamodel.SelfReferencingFK.ForeignKeyName` | `derived` | part of that derived record |
| `schemamodel.SelfReferencingFK.OnDelete` | `derived` | part of that derived record |
| `schemamodel.SelfReferencingFK.OnUpdate` | `derived` | part of that derived record |
| `schemamodel.Sequence.AsType` | `ddl` | — |
| `schemamodel.Sequence.Cache` | `ddl` | — |
| `schemamodel.Sequence.Comment` | `ddl` | — |
| `schemamodel.Sequence.Cycle` | `ddl` | — |
| `schemamodel.Sequence.Dialects` | `ddl` | — |
| `schemamodel.Sequence.Facets` | `ddl` | — |
| `schemamodel.Sequence.IfNotExists` | `ddl` | — |
| `schemamodel.Sequence.Increment` | `ddl` | — |
| `schemamodel.Sequence.MaxValue` | `ddl` | — |
| `schemamodel.Sequence.MinValue` | `ddl` | — |
| `schemamodel.Sequence.Name` | `ddl` | — |
| `schemamodel.Sequence.OwnedBy` | `ddl` | — |
| `schemamodel.Sequence.Schema` | `ddl` | — |
| `schemamodel.Sequence.Start` | `ddl` | — |
| `schemamodel.Sequence.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Table.APIName` | `export` | the name an exported API document carries when it differs from the database name |
| `schemamodel.Table.APINames` | `export` | the per-format names an exported API document carries, overriding the general one |
| `schemamodel.Table.Checks` | `ddl` | — |
| `schemamodel.Table.Collate` | `ddl` | — |
| `schemamodel.Table.Comment` | `ddl` | — |
| `schemamodel.Table.CustomSQL` | `ddl` | — |
| `schemamodel.Table.DependsOn` | `planning` | an ordering edge the author declares because no foreign key states it; the CREATE TABLE it orders does not mention it |
| `schemamodel.Table.Engine` | `ddl` | — |
| `schemamodel.Table.Facets` | `ddl` | — |
| `schemamodel.Table.Name` | `ddl` | — |
| `schemamodel.Table.Overrides` | `ddl` | — |
| `schemamodel.Table.PrimaryKey` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyComment` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyDeferrable` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyFacets` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyInclude` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyInitially` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyMethod` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyName` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyParts` | `ddl` | — |
| `schemamodel.Table.Schema` | `ddl` | — |
| `schemamodel.Table.StructName` | `ddl` | — |
| `schemamodel.TargetNames.GraphQL` | `export` | the name one export format carries, overriding the general one |
| `schemamodel.TargetNames.OpenAPI` | `export` | the name one export format carries, overriding the general one |
| `schemamodel.TargetNames.Protobuf` | `export` | the name one export format carries, overriding the general one |
| `schemamodel.Trigger.Body` | `ddl` | — |
| `schemamodel.Trigger.Comment` | `ddl` | — |
| `schemamodel.Trigger.Dialects` | `ddl` | — |
| `schemamodel.Trigger.Event` | `ddl` | — |
| `schemamodel.Trigger.ExecuteFunction` | `ddl` | — |
| `schemamodel.Trigger.Facets` | `ddl` | — |
| `schemamodel.Trigger.ForEach` | `ddl` | — |
| `schemamodel.Trigger.Name` | `ddl` | — |
| `schemamodel.Trigger.NewTable` | `ddl` | — |
| `schemamodel.Trigger.OldTable` | `ddl` | — |
| `schemamodel.Trigger.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Trigger.Table` | `ddl` | — |
| `schemamodel.Trigger.Timing` | `ddl` | — |
| `schemamodel.Trigger.When` | `ddl` | — |
| `schemamodel.View.Attributes` | `ddl` | — |
| `schemamodel.View.Body` | `ddl` | — |
| `schemamodel.View.Comment` | `ddl` | — |
| `schemamodel.View.DependsOn` | `planning` | an ordering edge the author declares because the view's body does not reveal it; the CREATE VIEW it orders does not mention it |
| `schemamodel.View.Dialects` | `ddl` | — |
| `schemamodel.View.Facets` | `ddl` | — |
| `schemamodel.View.Name` | `ddl` | — |
| `schemamodel.View.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.View.WithCheck` | `ddl` | — |
| `spannerschema.DesiredRowDeletion.Policy` | `ddl` | — |
| `spannerschema.Policy.Column` | `ddl` | — |
| `spannerschema.Policy.Interval` | `ddl` | — |
| `sqlitetable.DesiredTable.Options` | `ddl` | — |
| `sqlitetable.DesiredVirtual.Virtual` | `ddl` | — |
| `sqlitetable.Options.Strict` | `ddl` | — |
| `sqlitetable.Options.WithoutRowID` | `ddl` | — |
| `sqlitetable.Virtual.Arguments` | `ddl` | — |
| `sqlitetable.Virtual.Module` | `ddl` | — |
| `synonym.DesiredSynonym.Comment` | `ddl` | — |
| `synonym.DesiredSynonym.StructName` | `source` | the Go struct the declaration was read from; the alias is its identity |
| `synonym.DesiredSynonym.Synonym` | `ddl` | — |
| `synonym.Synonym.Name` | `ddl` | — |
| `synonym.Synonym.Schema` | `ddl` | — |
| `synonym.Synonym.Target` | `ddl` | — |
| `tsschema.DesiredContinuousAggregate.Body` | `ddl` | — |
| `tsschema.DesiredContinuousAggregate.Comment` | `ddl` | — |
| `tsschema.DesiredContinuousAggregate.MaterializedOnly` | `ddl` | — |
| `tsschema.DesiredContinuousAggregate.Normalized` | `comparison` | a connected server's rewrite of the declared body, attached before a live comparison; a statement writes the body as declared |
| `tsschema.DesiredContinuousAggregate.StructName` | `source` | the Go struct the declaration was read from; the aggregate's schema and name are its identity |
| `tsschema.DesiredHypertable.ChunkInterval` | `ddl` | — |
| `tsschema.DesiredHypertable.Column` | `ddl` | — |
| `tsschema.DesiredHypertable.Comment` | `ddl` | — |
| `tsschema.DesiredHypertable.IfNotExists` | `ddl` | — |
| `tsschema.NormalizedBody.Body` | `comparison` | the server's spelling of a declared body, compared with the catalog definition and never rendered |
| `ydbcoordination.Desired.Spec` | `ddl` | — |
| `ydbcoordination.Desired.StructName` | `source` | the Go holder recorded by the source; node identity is independent of the holder |
| `ydbcoordination.Spec.AttachConsistencyMode` | `ddl` | — |
| `ydbcoordination.Spec.RateLimiterCountersMode` | `ddl` | — |
| `ydbcoordination.Spec.ReadConsistencyMode` | `ddl` | — |
| `ydbcoordination.Spec.SelfCheckPeriodMillis` | `ddl` | — |
| `ydbcoordination.Spec.SessionGracePeriodMillis` | `ddl` | — |
| `ydbexternal.Column.Name` | `ddl` | — |
| `ydbexternal.Column.NotNull` | `ddl` | — |
| `ydbexternal.Column.Type` | `ddl` | — |
| `ydbexternal.DataSource.AuthMethod` | `ddl` | — |
| `ydbexternal.DataSource.Location` | `ddl` | — |
| `ydbexternal.DataSource.Options` | `ddl` | — |
| `ydbexternal.DataSource.SourceType` | `ddl` | — |
| `ydbexternal.DesiredSource.Spec` | `ddl` | — |
| `ydbexternal.DesiredSource.StructName` | `source` | the annotation holder, independent of the data source's path |
| `ydbexternal.DesiredTable.Spec` | `ddl` | — |
| `ydbexternal.DesiredTable.StructName` | `source` | the annotation holder, independent of the external table's path |
| `ydbexternal.Table.Columns` | `ddl` | — |
| `ydbexternal.Table.DataSource` | `ddl` | — |
| `ydbexternal.Table.Location` | `ddl` | — |
| `ydbexternal.Table.Options` | `ddl` | — |
| `ydbreplication.Connection.ConnectionString` | `ddl` | — |
| `ydbreplication.Connection.PasswordSecretName` | `ddl` | — |
| `ydbreplication.Connection.PasswordSecretPath` | `ddl` | — |
| `ydbreplication.Connection.TokenSecretName` | `ddl` | — |
| `ydbreplication.Connection.TokenSecretPath` | `ddl` | — |
| `ydbreplication.Connection.User` | `ddl` | — |
| `ydbreplication.DesiredReplication.Spec` | `ddl` | — |
| `ydbreplication.DesiredReplication.StructName` | `source` | the annotation holder, independent of the replication's path |
| `ydbreplication.DesiredTransfer.Spec` | `ddl` | — |
| `ydbreplication.DesiredTransfer.StructName` | `source` | the annotation holder, independent of the transfer's path |
| `ydbreplication.Item.Source` | `ddl` | — |
| `ydbreplication.Item.Target` | `ddl` | — |
| `ydbreplication.ReplicationSpec.CommitInterval` | `ddl` | — |
| `ydbreplication.ReplicationSpec.Connection` | `ddl` | — |
| `ydbreplication.ReplicationSpec.ConsistencyLevel` | `ddl` | — |
| `ydbreplication.ReplicationSpec.Items` | `ddl` | — |
| `ydbreplication.TransferSpec.BatchSizeBytes` | `ddl` | — |
| `ydbreplication.TransferSpec.Connection` | `ddl` | — |
| `ydbreplication.TransferSpec.Consumer` | `ddl` | — |
| `ydbreplication.TransferSpec.FlushInterval` | `ddl` | — |
| `ydbreplication.TransferSpec.Lambda` | `ddl` | — |
| `ydbreplication.TransferSpec.Source` | `ddl` | — |
| `ydbreplication.TransferSpec.Target` | `ddl` | — |
| `ydbschema.ChangefeedSpec.Consumers` | `ddl` | — |
| `ydbschema.ChangefeedSpec.Disabled` | `ddl` | — |
| `ydbschema.ChangefeedSpec.Format` | `ddl` | — |
| `ydbschema.ChangefeedSpec.InitialScan` | `ddl` | — |
| `ydbschema.ChangefeedSpec.Mode` | `ddl` | — |
| `ydbschema.ChangefeedSpec.Name` | `ddl` | — |
| `ydbschema.ChangefeedSpec.ResolvedTimestamps` | `ddl` | — |
| `ydbschema.ChangefeedSpec.RetentionPeriod` | `ddl` | — |
| `ydbschema.ChangefeedSpec.SchemaChanges` | `ddl` | — |
| `ydbschema.ChangefeedSpec.TopicAutoPartitioning` | `ddl` | — |
| `ydbschema.ChangefeedSpec.TopicMinActivePartitions` | `ddl` | — |
| `ydbschema.ChangefeedSpec.UserSIDs` | `ddl` | — |
| `ydbschema.ChangefeedSpec.VirtualTimestamps` | `ddl` | — |
| `ydbschema.ColumnFamily.CacheMode` | `ddl` | — |
| `ydbschema.ColumnFamily.Columns` | `ddl` | — |
| `ydbschema.ColumnFamily.Compression` | `ddl` | — |
| `ydbschema.ColumnFamily.Data` | `ddl` | — |
| `ydbschema.ColumnFamily.KeepInMemory` | `comparison` | keep_in_memory as a read finds it on a YDB family; no YQL statement writes it, and it decides whether a change or a rebuild is refused |
| `ydbschema.ColumnFamily.Name` | `ddl` | — |
| `ydbschema.ColumnStore.HashColumns` | `ddl` | — |
| `ydbschema.ColumnStore.Partitions` | `ddl` | — |
| `ydbschema.ColumnStore.TTL` | `ddl` | — |
| `ydbschema.DesiredChangefeed.RetainedReplication` | `planning` | retains an observed controller binding and refuses independent changefeed creation or mutation |
| `ydbschema.DesiredChangefeed.Spec` | `ddl` | — |
| `ydbschema.DesiredColumnFamilies.Families` | `ddl` | — |
| `ydbschema.DesiredColumnStore.ColumnStore` | `ddl` | — |
| `ydbschema.DesiredIndexPartitioning.IndexPartitioning` | `ddl` | — |
| `ydbschema.DesiredTTL.Policy` | `ddl` | — |
| `ydbschema.DesiredTablePartitioning.TablePartitioning` | `ddl` | — |
| `ydbschema.DesiredVectorIndex.Clusters` | `ddl` | — |
| `ydbschema.DesiredVectorIndex.Dimension` | `ddl` | — |
| `ydbschema.DesiredVectorIndex.Distance` | `ddl` | — |
| `ydbschema.DesiredVectorIndex.Levels` | `ddl` | — |
| `ydbschema.DesiredVectorIndex.Similarity` | `ddl` | — |
| `ydbschema.DesiredVectorIndex.VectorType` | `ddl` | — |
| `ydbschema.IndexPartitioning.ByLoad` | `ddl` | — |
| `ydbschema.IndexPartitioning.BySize` | `ddl` | — |
| `ydbschema.IndexPartitioning.MaxPartitions` | `ddl` | — |
| `ydbschema.IndexPartitioning.MinPartitions` | `ddl` | — |
| `ydbschema.IndexPartitioning.PartitionSizeMB` | `ddl` | — |
| `ydbschema.IndexPartitioning.ReadReplicas` | `ddl` | — |
| `ydbschema.ReplicationBinding.DestinationPath` | `planning` | records the observed replica destination without interpreting it as a local replication object |
| `ydbschema.ReplicationBinding.ItemID` | `planning` | identifies the observed replication target item whose stream must not be managed independently |
| `ydbschema.ReplicationBinding.SupportsTopicAutopartitioning` | `planning` | preserves observed controller behavior through snapshots and codecs |
| `ydbschema.TTL.Column` | `ddl` | — |
| `ydbschema.TTL.Interval` | `ddl` | — |
| `ydbschema.TTL.Unit` | `ddl` | — |
| `ydbschema.TTLTier.ExternalSource` | `ddl` | — |
| `ydbschema.TTLTier.Interval` | `ddl` | — |
| `ydbschema.TablePartitioning.ByLoad` | `ddl` | — |
| `ydbschema.TablePartitioning.BySize` | `ddl` | — |
| `ydbschema.TablePartitioning.KeyBloomFilter` | `ddl` | — |
| `ydbschema.TablePartitioning.MaxPartitions` | `ddl` | — |
| `ydbschema.TablePartitioning.MinPartitions` | `ddl` | — |
| `ydbschema.TablePartitioning.PartitionAtKeys` | `ddl` | — |
| `ydbschema.TablePartitioning.PartitionSizeMB` | `ddl` | — |
| `ydbschema.TablePartitioning.ReadReplicas` | `ddl` | — |
| `ydbschema.TablePartitioning.UniformPartitions` | `ddl` | — |
| `ydbschema.TieredTTL.Column` | `ddl` | — |
| `ydbschema.TieredTTL.Tiers` | `ddl` | — |
| `ydbschema.TieredTTL.Unit` | `ddl` | — |
| `ydbsecret.Desired.StructName` | `source` | the annotation holder, independent of the secret's path |
| `ydbsecret.Desired.ValueEnv` | `ddl` | — |
| `ydbstreaming.Desired.AllowStateReset` | `ddl` | — |
| `ydbstreaming.Desired.Spec` | `ddl` | — |
| `ydbstreaming.Desired.StructName` | `source` | the annotation holder, independent of query identity |
| `ydbstreaming.Spec.ResourcePool` | `ddl` | — |
| `ydbstreaming.Spec.Run` | `ddl` | — |
| `ydbstreaming.Spec.Text` | `ddl` | — |
| `ydbtopic.ConsumerSpec.AvailabilityPeriod` | `ddl` | — |
| `ydbtopic.ConsumerSpec.Important` | `ddl` | — |
| `ydbtopic.ConsumerSpec.Name` | `ddl` | — |
| `ydbtopic.ConsumerSpec.ReadFrom` | `ddl` | — |
| `ydbtopic.ConsumerSpec.SupportedCodecs` | `ddl` | — |
| `ydbtopic.Desired.Spec` | `ddl` | — |
| `ydbtopic.Desired.StructName` | `source` | the annotation holder, independent of the topic's path |
| `ydbtopic.Spec.AutoPartitioningDownUtilizationPercent` | `ddl` | — |
| `ydbtopic.Spec.AutoPartitioningStabilizationWindow` | `ddl` | — |
| `ydbtopic.Spec.AutoPartitioningStrategy` | `ddl` | — |
| `ydbtopic.Spec.AutoPartitioningUpUtilizationPercent` | `ddl` | — |
| `ydbtopic.Spec.Consumers` | `ddl` | — |
| `ydbtopic.Spec.MaxActivePartitions` | `ddl` | — |
| `ydbtopic.Spec.MinActivePartitions` | `ddl` | — |
| `ydbtopic.Spec.PartitionWriteBurstBytes` | `ddl` | — |
| `ydbtopic.Spec.PartitionWriteSpeedBytesPerSecond` | `ddl` | — |
| `ydbtopic.Spec.RetentionPeriod` | `ddl` | — |
| `ydbtopic.Spec.SupportedCodecs` | `ddl` | — |
| `ydbworkload.ClassifierSpec.MemberName` | `ddl` | — |
| `ydbworkload.ClassifierSpec.Rank` | `ddl` | — |
| `ydbworkload.ClassifierSpec.ResourcePool` | `ddl` | — |
| `ydbworkload.DesiredClassifier.Spec` | `ddl` | — |
| `ydbworkload.DesiredClassifier.StructName` | `source` | the annotation holder, independent of classifier identity |
| `ydbworkload.DesiredPool.Spec` | `ddl` | — |
| `ydbworkload.DesiredPool.StructName` | `source` | the annotation holder, independent of pool identity |
| `ydbworkload.PoolSpec.ConcurrentQueryLimit` | `ddl` | — |
| `ydbworkload.PoolSpec.DatabaseLoadCPUThreshold` | `ddl` | — |
| `ydbworkload.PoolSpec.QueryCPULimitPercentPerNode` | `ddl` | — |
| `ydbworkload.PoolSpec.QueryMemoryLimitPercentPerNode` | `ddl` | — |
| `ydbworkload.PoolSpec.QueueSize` | `ddl` | — |
| `ydbworkload.PoolSpec.ResourceWeight` | `ddl` | — |
| `ydbworkload.PoolSpec.TotalCPULimitPercentPerNode` | `ddl` | — |
<!-- END GENERATED FIELD DISPOSITIONS -->

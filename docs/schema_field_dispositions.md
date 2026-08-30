# What every desired-schema field is for

A fact an author declares can disappear on the way to SQL, and the render still
exits 0. Five repairs for that shape landed in one week across four packages —
`internal/deporder`, the ClickHouse renderer, `internal/convert/dbschematogo`
and `internal/convert/fromschema` twice — each of them a converter that walked
the model field by field and a field nobody had taught it about.

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

It also measures the render path. A field read only by comparison or by the
planner is classified as such and carries its reason, and this census does not
watch those paths run.

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
347 fields are reachable from the desired schema, and each one carries
exactly one disposition.

| Disposition | Fields | What it means |
| --- | --- | --- |
| `ddl` | 296 | reaches rendered SQL on at least one target |
| `comparison` | 7 | read when two schemas are compared, and written into no statement |
| `planning` | 4 | read while a change set is assembled or ordered |
| `derived` | 10 | computed from other fields rather than authored |
| `source` | 14 | identifies the source text the declaration was read from |
| `export` | 9 | a name or shape an exported API document carries |
| `data` | 7 | reference or seed rows, which are not DDL |

### Fields that should render and do not

5, each recorded against the issue that tracks the repair. The gate
refuses a gap that has started rendering, so a repair fails the build until
its entry is reclassified.

| Field | Issue |
| --- | --- |
| `schemamodel.Constraint.Comment` | https://github.com/stokaro/ptah/issues/2611 |
| `schemamodel.Field.Enum` | https://github.com/stokaro/ptah/issues/2611 |
| `schemamodel.Field.UniqueExpr` | https://github.com/stokaro/ptah/issues/2611 |
| `schemamodel.Grant.GrantedBy` | https://github.com/stokaro/ptah/issues/2611 |
| `schemamodel.Table.CustomSQL` | https://github.com/stokaro/ptah/issues/2590 |

### Every field

| Field | Disposition | Why it is not rendered |
| --- | --- | --- |
| `ast.MatViewRefreshSpec.Append` | `ddl` | — |
| `ast.MatViewRefreshSpec.DependsOn` | `ddl` | — |
| `ast.MatViewRefreshSpec.Interval` | `ddl` | — |
| `ast.MatViewRefreshSpec.Mode` | `ddl` | — |
| `ast.MatViewRefreshSpec.Offset` | `ddl` | — |
| `ast.MatViewRefreshSpec.Randomize` | `ddl` | — |
| `ast.RowDeletionPolicySpec.Column` | `ddl` | — |
| `ast.RowDeletionPolicySpec.Interval` | `ddl` | — |
| `ast.RowTTLSpec.DeleteBatchSize` | `ddl` | — |
| `ast.RowTTLSpec.DeleteRateLimit` | `ddl` | — |
| `ast.RowTTLSpec.DisableChangefeedReplication` | `ddl` | — |
| `ast.RowTTLSpec.ExpirationExpression` | `ddl` | — |
| `ast.RowTTLSpec.ExpireAfter` | `ddl` | — |
| `ast.RowTTLSpec.JobCron` | `ddl` | — |
| `ast.RowTTLSpec.LabelMetrics` | `ddl` | — |
| `ast.RowTTLSpec.Pause` | `ddl` | — |
| `ast.RowTTLSpec.RowStatsPollInterval` | `ddl` | — |
| `ast.RowTTLSpec.SelectBatchSize` | `ddl` | — |
| `ast.RowTTLSpec.SelectRateLimit` | `ddl` | — |
| `coverage.Object.Kind` | `comparison` | which kind the undescribed object is |
| `coverage.Object.Name` | `comparison` | which object was not described |
| `coverage.Object.Provenance` | `comparison` | how Ptah learned the object was not described |
| `coverage.Object.Reason` | `comparison` | why it was not described |
| `coverage.Set.Objects` | `comparison` | the per-object half of that record |
| `schemamodel.CompositeField.Name` | `ddl` | — |
| `schemamodel.CompositeField.Type` | `ddl` | — |
| `schemamodel.CompositeType.Comment` | `ddl` | — |
| `schemamodel.CompositeType.Dialects` | `ddl` | — |
| `schemamodel.CompositeType.Fields` | `ddl` | — |
| `schemamodel.CompositeType.Name` | `ddl` | — |
| `schemamodel.CompositeType.Schema` | `ddl` | — |
| `schemamodel.CompositeType.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Constraint.CheckExpression` | `ddl` | — |
| `schemamodel.Constraint.Columns` | `ddl` | — |
| `schemamodel.Constraint.Comment` | `ddl` | — |
| `schemamodel.Constraint.Deferrable` | `ddl` | — |
| `schemamodel.Constraint.ExcludeElements` | `ddl` | — |
| `schemamodel.Constraint.ForeignColumn` | `ddl` | — |
| `schemamodel.Constraint.ForeignColumns` | `ddl` | — |
| `schemamodel.Constraint.ForeignTable` | `ddl` | — |
| `schemamodel.Constraint.IncludeColumns` | `ddl` | — |
| `schemamodel.Constraint.Initially` | `ddl` | — |
| `schemamodel.Constraint.Name` | `ddl` | — |
| `schemamodel.Constraint.NullsDistinct` | `ddl` | — |
| `schemamodel.Constraint.OnDelete` | `ddl` | — |
| `schemamodel.Constraint.OnUpdate` | `ddl` | — |
| `schemamodel.Constraint.RequiresExtensions` | `planning` | which extensions must exist before this constraint can be created; it orders the statements and appears in none of them |
| `schemamodel.Constraint.StructName` | `ddl` | — |
| `schemamodel.Constraint.Table` | `ddl` | — |
| `schemamodel.Constraint.Type` | `ddl` | — |
| `schemamodel.Constraint.UsingMethod` | `ddl` | — |
| `schemamodel.Constraint.WhereCondition` | `ddl` | — |
| `schemamodel.ContinuousAggregate.Body` | `ddl` | — |
| `schemamodel.ContinuousAggregate.Comment` | `ddl` | — |
| `schemamodel.ContinuousAggregate.MaterializedOnly` | `ddl` | — |
| `schemamodel.ContinuousAggregate.Name` | `ddl` | — |
| `schemamodel.ContinuousAggregate.Schema` | `ddl` | — |
| `schemamodel.ContinuousAggregate.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Database.CompositeTypes` | `ddl` | — |
| `schemamodel.Database.Constraints` | `ddl` | — |
| `schemamodel.Database.ContinuousAggregates` | `ddl` | — |
| `schemamodel.Database.Dependencies` | `derived` | table creation order, derived by Finalize from the declared foreign keys |
| `schemamodel.Database.Domains` | `ddl` | — |
| `schemamodel.Database.EmbeddedFields` | `ddl` | — |
| `schemamodel.Database.EmbeddedSources` | `ddl` | — |
| `schemamodel.Database.Enums` | `ddl` | — |
| `schemamodel.Database.ExtendedProperties` | `ddl` | — |
| `schemamodel.Database.Extensions` | `ddl` | — |
| `schemamodel.Database.Fields` | `ddl` | — |
| `schemamodel.Database.FunctionDependencies` | `derived` | function creation order, derived by Finalize from the declared bodies |
| `schemamodel.Database.Functions` | `ddl` | — |
| `schemamodel.Database.Grants` | `ddl` | — |
| `schemamodel.Database.Hypertables` | `ddl` | — |
| `schemamodel.Database.Indexes` | `ddl` | — |
| `schemamodel.Database.ManagedData` | `data` | reference and seed rows; `ptah seed` writes them and `ptah schema render` does not |
| `schemamodel.Database.MaterializedViews` | `ddl` | — |
| `schemamodel.Database.NotDescribed` | `comparison` | what the description does not claim to describe; it decides what a diff may conclude about an object nobody looked at, and reaches no statement |
| `schemamodel.Database.RLSEnabledTables` | `ddl` | — |
| `schemamodel.Database.RLSPolicies` | `ddl` | — |
| `schemamodel.Database.Ranges` | `ddl` | — |
| `schemamodel.Database.Roles` | `ddl` | — |
| `schemamodel.Database.Schemas` | `ddl` | — |
| `schemamodel.Database.SelfReferencingForeignKeys` | `derived` | derived by Finalize from the declared foreign keys, so the planner can create the table before the reference to itself |
| `schemamodel.Database.Sequences` | `ddl` | — |
| `schemamodel.Database.Synonyms` | `ddl` | — |
| `schemamodel.Database.Tables` | `ddl` | — |
| `schemamodel.Database.Triggers` | `ddl` | — |
| `schemamodel.Database.Views` | `ddl` | — |
| `schemamodel.Domain.BaseType` | `ddl` | — |
| `schemamodel.Domain.Check` | `ddl` | — |
| `schemamodel.Domain.Comment` | `ddl` | — |
| `schemamodel.Domain.Default` | `ddl` | — |
| `schemamodel.Domain.DefaultExpr` | `ddl` | — |
| `schemamodel.Domain.Dialects` | `ddl` | — |
| `schemamodel.Domain.Name` | `ddl` | — |
| `schemamodel.Domain.NotNull` | `ddl` | — |
| `schemamodel.Domain.Schema` | `ddl` | — |
| `schemamodel.Domain.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
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
| `schemamodel.Enum.Name` | `ddl` | — |
| `schemamodel.Enum.Schema` | `ddl` | — |
| `schemamodel.Enum.Values` | `ddl` | — |
| `schemamodel.ExtendedProperty.Column` | `ddl` | — |
| `schemamodel.ExtendedProperty.Comment` | `ddl` | — |
| `schemamodel.ExtendedProperty.Name` | `ddl` | — |
| `schemamodel.ExtendedProperty.Schema` | `ddl` | — |
| `schemamodel.ExtendedProperty.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.ExtendedProperty.Table` | `ddl` | — |
| `schemamodel.ExtendedProperty.Value` | `ddl` | — |
| `schemamodel.Extension.Comment` | `ddl` | — |
| `schemamodel.Extension.Dialects` | `ddl` | — |
| `schemamodel.Extension.IfNotExists` | `ddl` | — |
| `schemamodel.Extension.Name` | `ddl` | — |
| `schemamodel.Extension.Provides` | `planning` | what the extension supplies, so a declaration depending on it can be ordered after it |
| `schemamodel.Extension.Schema` | `ddl` | — |
| `schemamodel.Extension.Version` | `ddl` | — |
| `schemamodel.Field.APIExpose` | `export` | whether the column reaches an exported API contract, and in which direction; Ptah emits no runtime that could enforce it |
| `schemamodel.Field.APIName` | `export` | the name an exported API document carries when it differs from the database name |
| `schemamodel.Field.APINames` | `export` | the per-format names an exported API document carries, overriding the general one |
| `schemamodel.Field.APIType` | `export` | the type an exported document should project the column as, which changes no stored value |
| `schemamodel.Field.AutoInc` | `ddl` | — |
| `schemamodel.Field.Charset` | `ddl` | — |
| `schemamodel.Field.Check` | `ddl` | — |
| `schemamodel.Field.CheckName` | `ddl` | — |
| `schemamodel.Field.Collate` | `ddl` | — |
| `schemamodel.Field.Comment` | `ddl` | — |
| `schemamodel.Field.Default` | `ddl` | — |
| `schemamodel.Field.DefaultExpr` | `ddl` | — |
| `schemamodel.Field.DefaultSet` | `ddl` | — |
| `schemamodel.Field.Deferrable` | `ddl` | — |
| `schemamodel.Field.Enum` | `ddl` | — |
| `schemamodel.Field.FieldName` | `ddl` | — |
| `schemamodel.Field.Foreign` | `ddl` | — |
| `schemamodel.Field.ForeignKeyName` | `ddl` | — |
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
| `schemamodel.Field.UpdateExpression` | `ddl` | — |
| `schemamodel.Function.Body` | `ddl` | — |
| `schemamodel.Function.Comment` | `ddl` | — |
| `schemamodel.Function.Dialects` | `ddl` | — |
| `schemamodel.Function.Kind` | `ddl` | — |
| `schemamodel.Function.Language` | `ddl` | — |
| `schemamodel.Function.Name` | `ddl` | — |
| `schemamodel.Function.Parameters` | `ddl` | — |
| `schemamodel.Function.Returns` | `ddl` | — |
| `schemamodel.Function.Security` | `ddl` | — |
| `schemamodel.Function.Settings` | `ddl` | — |
| `schemamodel.Function.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Function.Volatility` | `ddl` | — |
| `schemamodel.Grant.Comment` | `ddl` | — |
| `schemamodel.Grant.Dialects` | `ddl` | — |
| `schemamodel.Grant.GrantedBy` | `ddl` | — |
| `schemamodel.Grant.OnSchema` | `ddl` | — |
| `schemamodel.Grant.OnSequence` | `ddl` | — |
| `schemamodel.Grant.OnTable` | `ddl` | — |
| `schemamodel.Grant.Privileges` | `ddl` | — |
| `schemamodel.Grant.Role` | `ddl` | — |
| `schemamodel.Grant.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Grant.WithOption` | `ddl` | — |
| `schemamodel.Hypertable.ChunkInterval` | `ddl` | — |
| `schemamodel.Hypertable.Column` | `ddl` | — |
| `schemamodel.Hypertable.Comment` | `ddl` | — |
| `schemamodel.Hypertable.IfNotExists` | `ddl` | — |
| `schemamodel.Hypertable.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Hypertable.Table` | `ddl` | — |
| `schemamodel.Index.Comment` | `ddl` | — |
| `schemamodel.Index.Concurrently` | `planning` | asks that the index be BUILT without locking when added to a live table; internal/concurrentindex owns that decision, and only a plan carries it into DDL |
| `schemamodel.Index.Condition` | `ddl` | — |
| `schemamodel.Index.Fields` | `ddl` | — |
| `schemamodel.Index.Granularity` | `ddl` | — |
| `schemamodel.Index.IncludeColumns` | `ddl` | — |
| `schemamodel.Index.Name` | `ddl` | — |
| `schemamodel.Index.NullsDistinct` | `ddl` | — |
| `schemamodel.Index.Operator` | `ddl` | — |
| `schemamodel.Index.Parser` | `ddl` | — |
| `schemamodel.Index.Parts` | `ddl` | — |
| `schemamodel.Index.RequiresExtensions` | `planning` | the same ordering fact for an index |
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
| `schemamodel.ManagedData.Schema` | `data` | part of the reference-row declaration; `ptah seed` reads it and no renderer does |
| `schemamodel.ManagedData.SourceDir` | `data` | part of the reference-row declaration; `ptah seed` reads it and no renderer does |
| `schemamodel.ManagedData.StructName` | `data` | part of the reference-row declaration; `ptah seed` reads it and no renderer does |
| `schemamodel.ManagedData.Table` | `data` | part of the reference-row declaration; `ptah seed` reads it and no renderer does |
| `schemamodel.MaterializedView.Body` | `ddl` | — |
| `schemamodel.MaterializedView.Comment` | `ddl` | — |
| `schemamodel.MaterializedView.Dialects` | `ddl` | — |
| `schemamodel.MaterializedView.Name` | `ddl` | — |
| `schemamodel.MaterializedView.Refresh` | `ddl` | — |
| `schemamodel.MaterializedView.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.PartitionPart.Expr` | `ddl` | — |
| `schemamodel.PartitionPart.Name` | `ddl` | — |
| `schemamodel.PartitionSpec.Parts` | `ddl` | — |
| `schemamodel.PartitionSpec.Type` | `ddl` | — |
| `schemamodel.PrimaryKeyPart.Desc` | `ddl` | — |
| `schemamodel.PrimaryKeyPart.Name` | `ddl` | — |
| `schemamodel.PrimaryKeyPart.Prefix` | `ddl` | — |
| `schemamodel.RLSEnabledTable.Comment` | `ddl` | — |
| `schemamodel.RLSEnabledTable.Dialects` | `ddl` | — |
| `schemamodel.RLSEnabledTable.StructName` | `ddl` | — |
| `schemamodel.RLSEnabledTable.Table` | `ddl` | — |
| `schemamodel.RLSPolicy.Comment` | `ddl` | — |
| `schemamodel.RLSPolicy.Dialects` | `ddl` | — |
| `schemamodel.RLSPolicy.Name` | `ddl` | — |
| `schemamodel.RLSPolicy.PolicyFor` | `ddl` | — |
| `schemamodel.RLSPolicy.StructName` | `ddl` | — |
| `schemamodel.RLSPolicy.Table` | `ddl` | — |
| `schemamodel.RLSPolicy.ToRoles` | `ddl` | — |
| `schemamodel.RLSPolicy.UsingExpression` | `ddl` | — |
| `schemamodel.RLSPolicy.WithCheckExpression` | `ddl` | — |
| `schemamodel.Range.Canonical` | `ddl` | — |
| `schemamodel.Range.ClearedAttributes` | `comparison` | records the attributes a declaration explicitly cleared, so a comparison can tell a value nobody wrote from one somebody removed |
| `schemamodel.Range.Collation` | `ddl` | — |
| `schemamodel.Range.Comment` | `ddl` | — |
| `schemamodel.Range.Dialects` | `ddl` | — |
| `schemamodel.Range.Name` | `ddl` | — |
| `schemamodel.Range.Schema` | `ddl` | — |
| `schemamodel.Range.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Range.Subtype` | `ddl` | — |
| `schemamodel.Range.SubtypeDiff` | `ddl` | — |
| `schemamodel.Range.SubtypeOpClass` | `ddl` | — |
| `schemamodel.Role.Comment` | `ddl` | — |
| `schemamodel.Role.CreateDB` | `ddl` | — |
| `schemamodel.Role.CreateRole` | `ddl` | — |
| `schemamodel.Role.Dialects` | `ddl` | — |
| `schemamodel.Role.Inherit` | `ddl` | — |
| `schemamodel.Role.Login` | `ddl` | — |
| `schemamodel.Role.Name` | `ddl` | — |
| `schemamodel.Role.Password` | `ddl` | — |
| `schemamodel.Role.Replication` | `ddl` | — |
| `schemamodel.Role.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Role.Superuser` | `ddl` | — |
| `schemamodel.Schema.Charset` | `ddl` | — |
| `schemamodel.Schema.Collate` | `ddl` | — |
| `schemamodel.Schema.Comment` | `ddl` | — |
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
| `schemamodel.Sequence.IfNotExists` | `ddl` | — |
| `schemamodel.Sequence.Increment` | `ddl` | — |
| `schemamodel.Sequence.MaxValue` | `ddl` | — |
| `schemamodel.Sequence.MinValue` | `ddl` | — |
| `schemamodel.Sequence.Name` | `ddl` | — |
| `schemamodel.Sequence.OwnedBy` | `ddl` | — |
| `schemamodel.Sequence.Schema` | `ddl` | — |
| `schemamodel.Sequence.Start` | `ddl` | — |
| `schemamodel.Sequence.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Synonym.Comment` | `ddl` | — |
| `schemamodel.Synonym.Name` | `ddl` | — |
| `schemamodel.Synonym.Schema` | `ddl` | — |
| `schemamodel.Synonym.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Synonym.Target` | `ddl` | — |
| `schemamodel.Table.APIName` | `export` | the name an exported API document carries when it differs from the database name |
| `schemamodel.Table.APINames` | `export` | the per-format names an exported API document carries, overriding the general one |
| `schemamodel.Table.AutoIncrement` | `ddl` | — |
| `schemamodel.Table.Charset` | `ddl` | — |
| `schemamodel.Table.Checks` | `ddl` | — |
| `schemamodel.Table.Collate` | `ddl` | — |
| `schemamodel.Table.Comment` | `ddl` | — |
| `schemamodel.Table.CustomSQL` | `ddl` | — |
| `schemamodel.Table.Engine` | `ddl` | — |
| `schemamodel.Table.Name` | `ddl` | — |
| `schemamodel.Table.Overrides` | `ddl` | — |
| `schemamodel.Table.Partition` | `ddl` | — |
| `schemamodel.Table.PrimaryKey` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyInclude` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyName` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyParts` | `ddl` | — |
| `schemamodel.Table.RowDeletionPolicy` | `ddl` | — |
| `schemamodel.Table.RowTTL` | `ddl` | — |
| `schemamodel.Table.Schema` | `ddl` | — |
| `schemamodel.Table.Strict` | `ddl` | — |
| `schemamodel.Table.StructName` | `ddl` | — |
| `schemamodel.Table.VirtualArguments` | `ddl` | — |
| `schemamodel.Table.VirtualModule` | `ddl` | — |
| `schemamodel.Table.WithoutRowID` | `ddl` | — |
| `schemamodel.TargetNames.GraphQL` | `export` | the name one export format carries, overriding the general one |
| `schemamodel.TargetNames.OpenAPI` | `export` | the name one export format carries, overriding the general one |
| `schemamodel.TargetNames.Protobuf` | `export` | the name one export format carries, overriding the general one |
| `schemamodel.Trigger.Body` | `ddl` | — |
| `schemamodel.Trigger.Comment` | `ddl` | — |
| `schemamodel.Trigger.Dialects` | `ddl` | — |
| `schemamodel.Trigger.Event` | `ddl` | — |
| `schemamodel.Trigger.ExecuteFunction` | `ddl` | — |
| `schemamodel.Trigger.ForEach` | `ddl` | — |
| `schemamodel.Trigger.Name` | `ddl` | — |
| `schemamodel.Trigger.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Trigger.Table` | `ddl` | — |
| `schemamodel.Trigger.Timing` | `ddl` | — |
| `schemamodel.View.Attributes` | `ddl` | — |
| `schemamodel.View.Body` | `ddl` | — |
| `schemamodel.View.Comment` | `ddl` | — |
| `schemamodel.View.Dialects` | `ddl` | — |
| `schemamodel.View.Name` | `ddl` | — |
| `schemamodel.View.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.View.WithCheck` | `ddl` | — |
<!-- END GENERATED FIELD DISPOSITIONS -->

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
600 fields are reachable from the desired schema, and each one carries
exactly one disposition.

| Disposition | Fields | What it means |
| --- | --- | --- |
| `ddl` | 519 | reaches rendered SQL on at least one target |
| `comparison` | 12 | read when two schemas are compared, and written into no statement |
| `planning` | 11 | read while a change set is assembled or ordered |
| `derived` | 10 | computed from other fields rather than authored |
| `source` | 27 | identifies the source text the declaration was read from |
| `export` | 10 | what a generated document carries, or reports that it cannot |
| `data` | 11 | reference or seed rows, which are not DDL |

### Fields that should render and do not

None.

### Every field

| Field | Disposition | Why it is not rendered |
| --- | --- | --- |
| `ast.AsyncReplicationItem.Source` | `ddl` | — |
| `ast.AsyncReplicationItem.Target` | `ddl` | — |
| `ast.AsyncReplicationSpec.CommitInterval` | `ddl` | — |
| `ast.AsyncReplicationSpec.Connection` | `ddl` | — |
| `ast.AsyncReplicationSpec.ConsistencyLevel` | `ddl` | — |
| `ast.AsyncReplicationSpec.Items` | `ddl` | — |
| `ast.IndexPartitioningSpec.ByLoad` | `ddl` | — |
| `ast.IndexPartitioningSpec.BySize` | `ddl` | — |
| `ast.IndexPartitioningSpec.MaxPartitions` | `ddl` | — |
| `ast.IndexPartitioningSpec.MinPartitions` | `ddl` | — |
| `ast.IndexPartitioningSpec.PartitionSizeMB` | `ddl` | — |
| `ast.IndexPartitioningSpec.ReadReplicas` | `ddl` | — |
| `ast.ReplicationConnectionSpec.ConnectionString` | `ddl` | — |
| `ast.ReplicationConnectionSpec.PasswordSecretName` | `ddl` | — |
| `ast.ReplicationConnectionSpec.PasswordSecretPath` | `ddl` | — |
| `ast.ReplicationConnectionSpec.TokenSecretName` | `ddl` | — |
| `ast.ReplicationConnectionSpec.TokenSecretPath` | `ddl` | — |
| `ast.ReplicationConnectionSpec.User` | `ddl` | — |
| `ast.TransferSpec.BatchSizeBytes` | `ddl` | — |
| `ast.TransferSpec.Connection` | `ddl` | — |
| `ast.TransferSpec.Consumer` | `ddl` | — |
| `ast.TransferSpec.FlushInterval` | `ddl` | — |
| `ast.TransferSpec.Lambda` | `ddl` | — |
| `ast.TransferSpec.Source` | `ddl` | — |
| `ast.TransferSpec.Target` | `ddl` | — |
| `ast.VectorIndexSpec.Clusters` | `ddl` | — |
| `ast.VectorIndexSpec.Dimension` | `ddl` | — |
| `ast.VectorIndexSpec.Distance` | `ddl` | — |
| `ast.VectorIndexSpec.Levels` | `ddl` | — |
| `ast.VectorIndexSpec.Similarity` | `ddl` | — |
| `ast.VectorIndexSpec.VectorType` | `ddl` | — |
| `ast.YDBColumnFamilySpec.CacheMode` | `ddl` | — |
| `ast.YDBColumnFamilySpec.Columns` | `ddl` | — |
| `ast.YDBColumnFamilySpec.Compression` | `ddl` | — |
| `ast.YDBColumnFamilySpec.Data` | `ddl` | — |
| `ast.YDBColumnFamilySpec.KeepInMemory` | `comparison` | keep_in_memory as a read finds it on a YDB family; no YQL statement writes it, and it decides whether a change or a rebuild is refused |
| `ast.YDBColumnFamilySpec.Name` | `ddl` | — |
| `ast.YDBColumnTableSpec.HashColumns` | `ddl` | — |
| `ast.YDBColumnTableSpec.Partitions` | `ddl` | — |
| `ast.YDBColumnTableSpec.TTL` | `ddl` | — |
| `ast.YDBTTLTierSpec.ExternalSource` | `ddl` | — |
| `ast.YDBTTLTierSpec.Interval` | `ddl` | — |
| `ast.YDBTablePartitioningSpec.ByLoad` | `ddl` | — |
| `ast.YDBTablePartitioningSpec.BySize` | `ddl` | — |
| `ast.YDBTablePartitioningSpec.KeyBloomFilter` | `ddl` | — |
| `ast.YDBTablePartitioningSpec.MaxPartitions` | `ddl` | — |
| `ast.YDBTablePartitioningSpec.MinPartitions` | `ddl` | — |
| `ast.YDBTablePartitioningSpec.PartitionAtKeys` | `ddl` | — |
| `ast.YDBTablePartitioningSpec.PartitionSizeMB` | `ddl` | — |
| `ast.YDBTablePartitioningSpec.ReadReplicas` | `ddl` | — |
| `ast.YDBTablePartitioningSpec.UniformPartitions` | `ddl` | — |
| `ast.YDBTieredTTLSpec.Column` | `ddl` | — |
| `ast.YDBTieredTTLSpec.Tiers` | `ddl` | — |
| `ast.YDBTieredTTLSpec.Unit` | `ddl` | — |
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
| `mssqlschema.DesiredSecurityPolicy.Enabled` | `ddl` | — |
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
| `schemamodel.AsyncReplication.Name` | `ddl` | — |
| `schemamodel.AsyncReplication.Schema` | `ddl` | — |
| `schemamodel.AsyncReplication.Spec` | `ddl` | — |
| `schemamodel.AsyncReplication.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.CompositeField.Name` | `ddl` | — |
| `schemamodel.CompositeField.Type` | `ddl` | — |
| `schemamodel.CompositeType.Comment` | `ddl` | — |
| `schemamodel.CompositeType.Dialects` | `ddl` | — |
| `schemamodel.CompositeType.Facets` | `ddl` | — |
| `schemamodel.CompositeType.Fields` | `ddl` | — |
| `schemamodel.CompositeType.Name` | `ddl` | — |
| `schemamodel.CompositeType.Schema` | `ddl` | — |
| `schemamodel.CompositeType.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Constraint.CheckExpression` | `ddl` | — |
| `schemamodel.Constraint.Columns` | `ddl` | — |
| `schemamodel.Constraint.Comment` | `ddl` | — |
| `schemamodel.Constraint.Deferrable` | `ddl` | — |
| `schemamodel.Constraint.ExcludeElements` | `ddl` | — |
| `schemamodel.Constraint.Facets` | `ddl` | — |
| `schemamodel.Constraint.ForeignColumn` | `ddl` | — |
| `schemamodel.Constraint.ForeignColumns` | `ddl` | — |
| `schemamodel.Constraint.ForeignTable` | `ddl` | — |
| `schemamodel.Constraint.IncludeColumns` | `ddl` | — |
| `schemamodel.Constraint.Initially` | `ddl` | — |
| `schemamodel.Constraint.KeyBlockSize` | `ddl` | — |
| `schemamodel.Constraint.Match` | `ddl` | — |
| `schemamodel.Constraint.Name` | `ddl` | — |
| `schemamodel.Constraint.NotEnforced` | `ddl` | — |
| `schemamodel.Constraint.NotValid` | `ddl` | — |
| `schemamodel.Constraint.NullsDistinct` | `ddl` | — |
| `schemamodel.Constraint.OnDelete` | `ddl` | — |
| `schemamodel.Constraint.OnDeleteColumns` | `ddl` | — |
| `schemamodel.Constraint.OnUpdate` | `ddl` | — |
| `schemamodel.Constraint.RequiresExtensions` | `planning` | which extensions must exist before this constraint can be created; it orders the statements and appears in none of them |
| `schemamodel.Constraint.StructName` | `ddl` | — |
| `schemamodel.Constraint.Table` | `ddl` | — |
| `schemamodel.Constraint.Type` | `ddl` | — |
| `schemamodel.Constraint.UsingMethod` | `ddl` | — |
| `schemamodel.Constraint.WhereCondition` | `ddl` | — |
| `schemamodel.Database.AsyncReplications` | `ddl` | — |
| `schemamodel.Database.CompositeTypes` | `ddl` | — |
| `schemamodel.Database.Constraints` | `ddl` | — |
| `schemamodel.Database.DatabasePath` | `ddl` | — |
| `schemamodel.Database.DefaultPrivileges` | `ddl` | — |
| `schemamodel.Database.Dependencies` | `derived` | table creation order, derived by Finalize from the declared foreign keys |
| `schemamodel.Database.Domains` | `ddl` | — |
| `schemamodel.Database.EmbeddedFields` | `ddl` | — |
| `schemamodel.Database.EmbeddedSources` | `ddl` | — |
| `schemamodel.Database.Enums` | `ddl` | — |
| `schemamodel.Database.ExtendedProperties` | `ddl` | — |
| `schemamodel.Database.Extensions` | `ddl` | — |
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
| `schemamodel.Database.Ranges` | `ddl` | — |
| `schemamodel.Database.RevokedGrants` | `ddl` | — |
| `schemamodel.Database.Roles` | `ddl` | — |
| `schemamodel.Database.Schemas` | `ddl` | — |
| `schemamodel.Database.SelfReferencingForeignKeys` | `derived` | derived by Finalize from the declared foreign keys, so the planner can create the table before the reference to itself |
| `schemamodel.Database.Sequences` | `ddl` | — |
| `schemamodel.Database.Synonyms` | `ddl` | — |
| `schemamodel.Database.Tables` | `ddl` | — |
| `schemamodel.Database.Transfers` | `ddl` | — |
| `schemamodel.Database.Triggers` | `ddl` | — |
| `schemamodel.Database.Views` | `ddl` | — |
| `schemamodel.DefaultPrivilege.Comment` | `ddl` | — |
| `schemamodel.DefaultPrivilege.Dialects` | `ddl` | — |
| `schemamodel.DefaultPrivilege.Grantee` | `ddl` | — |
| `schemamodel.DefaultPrivilege.Grantor` | `ddl` | — |
| `schemamodel.DefaultPrivilege.ObjectType` | `ddl` | — |
| `schemamodel.DefaultPrivilege.Privileges` | `ddl` | — |
| `schemamodel.DefaultPrivilege.Revoked` | `ddl` | — |
| `schemamodel.DefaultPrivilege.Schema` | `ddl` | — |
| `schemamodel.DefaultPrivilege.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Domain.BaseType` | `ddl` | — |
| `schemamodel.Domain.Check` | `ddl` | — |
| `schemamodel.Domain.Comment` | `ddl` | — |
| `schemamodel.Domain.Default` | `ddl` | — |
| `schemamodel.Domain.DefaultExpr` | `ddl` | — |
| `schemamodel.Domain.Dialects` | `ddl` | — |
| `schemamodel.Domain.Facets` | `ddl` | — |
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
| `schemamodel.Enum.Comment` | `ddl` | — |
| `schemamodel.Enum.Facets` | `ddl` | — |
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
| `schemamodel.Field.UpdateExpression` | `ddl` | — |
| `schemamodel.Function.Body` | `ddl` | — |
| `schemamodel.Function.Comment` | `ddl` | — |
| `schemamodel.Function.Dialects` | `ddl` | — |
| `schemamodel.Function.Facets` | `ddl` | — |
| `schemamodel.Function.Kind` | `ddl` | — |
| `schemamodel.Function.Language` | `ddl` | — |
| `schemamodel.Function.Leakproof` | `ddl` | — |
| `schemamodel.Function.Name` | `ddl` | — |
| `schemamodel.Function.Parallel` | `ddl` | — |
| `schemamodel.Function.Parameters` | `ddl` | — |
| `schemamodel.Function.Returns` | `ddl` | — |
| `schemamodel.Function.Security` | `ddl` | — |
| `schemamodel.Function.Settings` | `ddl` | — |
| `schemamodel.Function.Strict` | `ddl` | — |
| `schemamodel.Function.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
| `schemamodel.Function.Volatility` | `ddl` | — |
| `schemamodel.Grant.Columns` | `ddl` | — |
| `schemamodel.Grant.Comment` | `ddl` | — |
| `schemamodel.Grant.Dialects` | `ddl` | — |
| `schemamodel.Grant.GrantedBy` | `export` | the grantor a catalog read observed, carried so that a generated document can report it cannot represent one; PostgreSQL accepts GRANTED BY only for the role that IS the current user, so rendering the observed grantor would fail on every apply by another role |
| `schemamodel.Grant.OnDatabase` | `ddl` | — |
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
| `schemamodel.Index.KeyBlockSize` | `ddl` | — |
| `schemamodel.Index.Name` | `ddl` | — |
| `schemamodel.Index.NullsDistinct` | `ddl` | — |
| `schemamodel.Index.Operator` | `ddl` | — |
| `schemamodel.Index.Overrides` | `ddl` | — |
| `schemamodel.Index.Parser` | `ddl` | — |
| `schemamodel.Index.Partitioning` | `ddl` | — |
| `schemamodel.Index.Parts` | `ddl` | — |
| `schemamodel.Index.RequiresExtensions` | `planning` | the same ordering fact for an index |
| `schemamodel.Index.StorageParams` | `ddl` | — |
| `schemamodel.Index.StructName` | `ddl` | — |
| `schemamodel.Index.TableName` | `ddl` | — |
| `schemamodel.Index.Type` | `ddl` | — |
| `schemamodel.Index.Unique` | `ddl` | — |
| `schemamodel.Index.Vector` | `ddl` | — |
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
| `schemamodel.PartitionPart.Expr` | `ddl` | — |
| `schemamodel.PartitionPart.Name` | `ddl` | — |
| `schemamodel.PartitionSpec.Parts` | `ddl` | — |
| `schemamodel.PartitionSpec.Type` | `ddl` | — |
| `schemamodel.PrimaryKeyPart.Desc` | `ddl` | — |
| `schemamodel.PrimaryKeyPart.Name` | `ddl` | — |
| `schemamodel.PrimaryKeyPart.Prefix` | `ddl` | — |
| `schemamodel.PrivilegeGrant.Privilege` | `ddl` | — |
| `schemamodel.PrivilegeGrant.WithOption` | `ddl` | — |
| `schemamodel.RLSEnabledTable.Comment` | `ddl` | — |
| `schemamodel.RLSEnabledTable.Dialects` | `ddl` | — |
| `schemamodel.RLSEnabledTable.Forced` | `ddl` | — |
| `schemamodel.RLSEnabledTable.StructName` | `ddl` | — |
| `schemamodel.RLSEnabledTable.Table` | `ddl` | — |
| `schemamodel.RLSPolicy.Comment` | `ddl` | — |
| `schemamodel.RLSPolicy.Dialects` | `ddl` | — |
| `schemamodel.RLSPolicy.Name` | `ddl` | — |
| `schemamodel.RLSPolicy.PolicyFor` | `ddl` | — |
| `schemamodel.RLSPolicy.Restrictive` | `ddl` | — |
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
| `schemamodel.Range.Facets` | `ddl` | — |
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
| `schemamodel.Table.DependsOn` | `planning` | an ordering edge the author declares because no foreign key states it; the CREATE TABLE it orders does not mention it |
| `schemamodel.Table.Engine` | `ddl` | — |
| `schemamodel.Table.Facets` | `ddl` | — |
| `schemamodel.Table.Name` | `ddl` | — |
| `schemamodel.Table.Overrides` | `ddl` | — |
| `schemamodel.Table.Partition` | `ddl` | — |
| `schemamodel.Table.PrimaryKey` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyBlockSize` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyComment` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyDeferrable` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyInclude` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyInitially` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyMethod` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyName` | `ddl` | — |
| `schemamodel.Table.PrimaryKeyParts` | `ddl` | — |
| `schemamodel.Table.Schema` | `ddl` | — |
| `schemamodel.Table.Strict` | `ddl` | — |
| `schemamodel.Table.StructName` | `ddl` | — |
| `schemamodel.Table.Unlogged` | `ddl` | — |
| `schemamodel.Table.VirtualArguments` | `ddl` | — |
| `schemamodel.Table.VirtualModule` | `ddl` | — |
| `schemamodel.Table.WithoutRowID` | `ddl` | — |
| `schemamodel.Table.YDBColumnFamilies` | `ddl` | — |
| `schemamodel.Table.YDBColumnTable` | `ddl` | — |
| `schemamodel.Table.YDBPartitioning` | `ddl` | — |
| `schemamodel.TargetNames.GraphQL` | `export` | the name one export format carries, overriding the general one |
| `schemamodel.TargetNames.OpenAPI` | `export` | the name one export format carries, overriding the general one |
| `schemamodel.TargetNames.Protobuf` | `export` | the name one export format carries, overriding the general one |
| `schemamodel.Transfer.Name` | `ddl` | — |
| `schemamodel.Transfer.Schema` | `ddl` | — |
| `schemamodel.Transfer.Spec` | `ddl` | — |
| `schemamodel.Transfer.StructName` | `source` | the Go struct the declaration was read from; the object's own name is its identity |
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
| `ydbschema.DesiredChangefeed.RetainedReplication` | `planning` | retains an observed controller binding and refuses independent changefeed creation or mutation |
| `ydbschema.DesiredChangefeed.Spec` | `ddl` | — |
| `ydbschema.DesiredTTL.Policy` | `ddl` | — |
| `ydbschema.ReplicationBinding.DestinationPath` | `planning` | records the observed replica destination without interpreting it as a local replication object |
| `ydbschema.ReplicationBinding.ItemID` | `planning` | identifies the observed replication target item whose stream must not be managed independently |
| `ydbschema.ReplicationBinding.SupportsTopicAutopartitioning` | `planning` | preserves observed controller behavior through snapshots and codecs |
| `ydbschema.TTL.Column` | `ddl` | — |
| `ydbschema.TTL.Interval` | `ddl` | — |
| `ydbschema.TTL.Unit` | `ddl` | — |
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

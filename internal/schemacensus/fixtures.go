package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/feature/synonym"
	"ptah.run/internal/capabilityprobe"
)

// Fixture is one named desired schema the census ablates fields out of.
//
// The set is many small schemas rather than a few large ones, and the reason is
// measured: a fixture a target refuses for an unrelated reason answers every
// ablation with the same refusal, and a field inside it reads as unobservable
// when nothing has been measured about it at all. One concern per fixture keeps
// a refusal about the concern.
type Fixture struct {
	Name   string
	Schema schemamodel.Database
	// Flags are capabilities a YDB cluster turns on with a feature flag,
	// off on every YDB preset, that the fixture's objects need. Every YDB
	// cell renders the fixture with them on, as a cluster that turned the
	// flags on would; a fixture without them would be refused on every cell,
	// and no field of it could be measured.
	Flags []capability.Capability
}

// Cells returns cells with the fixture's flags turned on in the preset of
// every YDB cell. The other cells are returned as they are.
func (f Fixture) Cells(cells []capabilityprobe.Cell) []capabilityprobe.Cell {
	if len(f.Flags) == 0 {
		return cells
	}
	flagged := make([]capabilityprobe.Cell, 0, len(cells))
	for _, cell := range cells {
		if cell.Dialect == platform.YDB && cell.Preset != nil {
			preset := cell.Preset
			cell.Preset = func() capability.Capabilities {
				caps := preset()
				for _, flag := range f.Flags {
					caps = caps.With(flag, true)
				}
				return caps
			}
		}
		flagged = append(flagged, cell)
	}
	return flagged
}

// oneTable is the smallest schema every target accepts: one table with a
// primary key, which ClickHouse needs for its ORDER BY and every other engine
// takes.
func oneTable(name string, table schemamodel.Table, extra ...schemamodel.Field) schemamodel.Database {
	fields := []schemamodel.Field{
		{StructName: name, FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
	}
	fields = append(fields, extra...)
	table.StructName = name
	return schemamodel.Database{
		Tables: []schemamodel.Table{table},
		Fields: fields,
	}
}

// Fixtures is the corpus. A field no fixture populates cannot be measured, and
// the gate reports that as its own state rather than as a loss.
func Fixtures() []Fixture {
	return withFacetFixtures([]Fixture{
		{Name: "table-clickhouse-settings", Schema: tableClickHouseSettingsFixture()},
		{Name: "schema", Schema: schemaFixture()},
		{Name: "column-core", Schema: columnCoreFixture()},
		{Name: "column-default", Schema: columnDefaultFixture()},
		{Name: "column-check", Schema: columnCheckFixture()},
		{Name: "column-unique", Schema: columnUniqueFixture()},
		{Name: "column-notnull-name", Schema: columnNotNullNameFixture()},
		{Name: "column-identity", Schema: columnIdentityFixture()},
		{Name: "column-autoinc", Schema: columnAutoIncFixture()},
		{Name: "column-generated", Schema: columnGeneratedFixture()},
		{Name: "column-enum", Schema: columnEnumFixture()},
		{Name: "column-enum-foreign-key", Schema: columnEnumForeignKeyFixture()},
		{Name: "column-mysql", Schema: columnMySQLFixture()},
		{Name: "column-declared-text", Schema: columnDeclaredTextFixture()},
		{Name: "column-raw-type", Schema: columnRawTypeFixture()},
		{Name: "column-api", Schema: columnAPIFixture()},
		{Name: "column-override", Schema: columnOverrideFixture()},
		{Name: "table-comment", Schema: tableCommentFixture()},
		{Name: "table-checks", Schema: tableChecksFixture()},
		{Name: "table-custom-sql", Schema: tableCustomSQLFixture()},
		{Name: "table-pk", Schema: tablePrimaryKeyFixture()},
		{Name: "table-pk-include", Schema: tablePrimaryKeyIncludeFixture()},
		{Name: "table-pk-deferrable", Schema: tablePrimaryKeyDeferrableFixture()},
		{Name: "table-pk-method", Schema: tablePrimaryKeyMethodFixture()},
		{Name: "table-pk-options", Schema: primaryKeyOptionsFixture()},
		{Name: "constraint-pk-options", Schema: primaryKeyConstraintOptionsFixture()},
		{Name: "table-pk-parts", Schema: tablePrimaryKeyPartsFixture()},
		{Name: "table-partition", Schema: tablePartitionFixture()},
		{Name: "table-mysql", Schema: tableMySQLFixture()},
		{Name: "table-engine", Schema: tableEngineFixture()},
		{Name: "table-sqlite", Schema: tableSQLiteFixture()},
		{Name: "table-unlogged", Schema: tableUnloggedFixture()},
		{Name: "table-virtual", Schema: tableVirtualFixture()},
		{Name: "table-api", Schema: tableAPIFixture()},
		{Name: "table-override", Schema: tableOverrideFixture()},
		{Name: "table-rowttl", Schema: tableRowTTLFixture()},
		{Name: "table-row-deletion", Schema: tableRowDeletionFixture()},
		{Name: "table-ydb-ttl", Schema: tableYDBTTLFixture()},
		{Name: "table-column-families", Schema: tableColumnFamiliesFixture()},
		{Name: "table-column-store", Schema: tableColumnStoreFixture(), Flags: []capability.Capability{capability.TieredTTL}},
		{Name: "table-changefeed", Schema: tableChangefeedFixture()},
		{Name: "table-changefeed-disabled", Schema: tableChangefeedDisabledFixture()},
		{Name: "table-row-deletion-epoch", Schema: tableRowDeletionEpochFixture()},
		{Name: "fk-field", Schema: foreignKeyFieldFixture()},
		{Name: "fk-field-deferrable", Schema: foreignKeyDeferrableFixture()},
		{Name: "fk-table", Schema: foreignKeyTableFixture()},
		{Name: "fk-table-composite", Schema: foreignKeyCompositeFixture()},
		{Name: "constraint-check", Schema: constraintCheckFixture()},
		{Name: "constraint-unique", Schema: constraintUniqueFixture()},
		{Name: "constraint-unique-include", Schema: constraintUniqueIncludeFixture()},
		{Name: "constraint-pk", Schema: constraintPrimaryKeyFixture()},
		{Name: "constraint-exclude", Schema: constraintExcludeFixture()},
		{Name: "index-basic", Schema: indexBasicFixture()},
		{Name: "index-partial", Schema: indexPartialFixture()},
		{Name: "index-nulls-not-distinct", Schema: indexNullsNotDistinctFixture()},
		{Name: "index-include", Schema: indexIncludeFixture()},
		{Name: "index-parts", Schema: indexPartsFixture()},
		{Name: "index-storage", Schema: indexStorageFixture()},
		{Name: "index-concurrent", Schema: indexConcurrentFixture()},
		{Name: "index-clickhouse", Schema: indexClickHouseFixture()},
		{Name: "index-clickhouse-settings", Schema: indexClickHouseSettingsFixture()},
		{Name: "index-fulltext", Schema: indexFullTextFixture()},
		{Name: "index-mysql-parser", Schema: indexMySQLParserFixture()},
		{Name: "index-invisible", Schema: indexInvisibleFixture()},
		{Name: "index-key-block-size", Schema: indexKeyBlockSizeFixture()},
		{Name: "index-partitioning", Schema: indexPartitioningFixture()},
		{Name: "index-partitioning-unsplit", Schema: indexPartitioningUnsplitFixture()},
		{Name: "index-vector", Schema: indexVectorFixture(&ydbschema.DesiredVectorIndex{
			Distance: "cosine", VectorType: "float", Dimension: 3, Levels: 2, Clusters: 128,
		})},
		{Name: "index-vector-similarity", Schema: indexVectorFixture(&ydbschema.DesiredVectorIndex{
			Similarity: "inner_product", VectorType: "int8", Dimension: 3, Levels: 1, Clusters: 2,
		})},
		{Name: "table-partitioning", Schema: tablePartitioningFixture()},
		{Name: "table-partitioning-unsplit", Schema: tablePartitioningUnsplitFixture()},
		{Name: "table-uniform-partitions", Schema: tableUniformPartitionsFixture()},
		{Name: "table-partition-at-keys", Schema: tablePartitionAtKeysFixture()},
		{Name: "enum", Schema: enumFixture()},
		{Name: "domain", Schema: domainFixture()},
		{Name: "composite", Schema: compositeFixture()},
		{Name: "range", Schema: rangeFixture()},
		{Name: "sequence", Schema: sequenceFixture()},
		{Name: "extension", Schema: extensionFixture()},
		{Name: "view", Schema: viewFixture()},
		{Name: "matview", Schema: matViewFixture()},
		{Name: "matview-refresh", Schema: matViewRefreshFixture()},
		{Name: "table-row-policy", Schema: tableRowPolicyFixture()},
		{Name: "table-row-policy-all-except", Schema: tableRowPolicyAllExceptFixture()},
		{Name: "function", Schema: functionFixture()},
		{Name: "function-planner-properties", Schema: functionPlannerPropertiesFixture()},
		{Name: "trigger", Schema: triggerFixture()},
		{Name: "hypertable", Schema: hypertableFixture()},
		{Name: "continuous-aggregate", Schema: continuousAggregateFixture()},
		{Name: "synonym", Schema: synonymFixture()},
		{Name: "topic", Schema: topicFixture()},
		{Name: "resource-pool", Schema: resourcePoolFixture()},
		{Name: "async-replication", Schema: asyncReplicationFixture()},
		{Name: "async-replication-token", Schema: asyncReplicationTokenFixture()},
		{Name: "transfer", Schema: transferFixture()},
		{Name: "owned-coordination-node", Schema: ownedCoordinationNodeFixture()},
		{Name: "secret", Schema: secretFixture()},
		{Name: "security-policy", Schema: securityPolicyFixture()},
		{Name: "pgpolicy-policy", Schema: tablePolicyFixture()},
		{Name: "pgpolicy-table-state", Schema: tableRowSecurityFixture()},
		{Name: "streaming-query", Schema: streamingQueryFixture(), Flags: []capability.Capability{capability.StreamingQueries}},
		{Name: "external-objects", Schema: externalObjectsFixture(),
			Flags: []capability.Capability{capability.ExternalDataSources}},
		{Name: "extended-property", Schema: extendedPropertyFixture()},
		{Name: "role", Schema: roleFixture()},
		{Name: "ydb-group-membership", Schema: ydbGroupMembershipFixture()},
		{Name: "ydb-grant-database", Schema: ydbGrantDatabaseFixture()},
		{Name: "grant-table", Schema: grantTableFixture()},
		{Name: "grant-schema", Schema: grantSchemaFixture()},
		{Name: "grant-sequence", Schema: grantSequenceFixture()},
		{Name: "grant-routine", Schema: grantRoutineFixture()},
		{Name: "revoked-grant", Schema: revokedGrantFixture()},
		{Name: "grant-columns", Schema: grantColumnsFixture()},
		{Name: "default-privilege-tables", Schema: defaultPrivilegeTablesFixture()},
		{Name: "default-privilege-sequences", Schema: defaultPrivilegeSequencesFixture()},
		{Name: "default-privilege-functions", Schema: defaultPrivilegeFunctionsFixture()},
		{Name: "default-privilege-types", Schema: defaultPrivilegeTypesFixture()},
		{Name: "default-privilege-grantable", Schema: defaultPrivilegeGrantableFixture()},
		{Name: "default-privilege-global", Schema: defaultPrivilegeGlobalFixture()},
		{Name: "rls", Schema: rlsFixture()},
		{Name: "rls-strength", Schema: rlsStrengthFixture()},
		{Name: "embedded-json", Schema: embeddedJSONFixture()},
		{Name: "embedded-relation", Schema: embeddedRelationFixture()},
		{Name: "embedded-inline", Schema: embeddedInlineFixture()},
		{Name: "managed-data", Schema: managedDataFixture()},
		{Name: "coverage", Schema: coverageFixture()},
		{Name: "column-default-empty", Schema: columnDefaultEmptyFixture()},
		{Name: "column-default-expr-only", Schema: columnDefaultExprOnlyFixture()},
		{Name: "column-unique-expr", Schema: columnUniqueExprFixture()},
		{Name: "fk-field-deferrable-only", Schema: foreignKeyDeferrableOnlyFixture()},
		{Name: "fk-field-initially-only", Schema: foreignKeyInitiallyOnlyFixture()},
		{Name: "foreign-key-self-field", Schema: selfReferencingForeignKeyFieldFixture()},
		{Name: "foreign-key-self-constraint", Schema: selfReferencingForeignKeyConstraintFixture()},
		{Name: "constraint-deferrable-only", Schema: constraintDeferrableOnlyFixture()},
		{Name: "constraint-initially-only", Schema: constraintInitiallyOnlyFixture()},
		{Name: "field-check-not-enforced", Schema: fieldCheckNotEnforcedFixture()},
		{Name: "fk-field-match", Schema: foreignKeyMatchFixture()},
		{Name: "fk-field-not-enforced", Schema: foreignKeyNotEnforcedFixture()},
		{Name: "constraint-check-not-enforced", Schema: constraintCheckNotEnforcedFixture()},
		{Name: "constraint-fk-match", Schema: constraintForeignKeyMatchFixture()},
		{Name: "constraint-check-not-valid", Schema: constraintCheckNotValidFixture()},
		{Name: "constraint-delete-column-list", Schema: constraintDeleteColumnListFixture()},
		{Name: "constraint-host-table-only", Schema: constraintHostTableOnlyFixture()},
		{Name: "index-host-table-only", Schema: indexHostTableOnlyFixture()},
		{Name: "domain-default-only", Schema: domainDefaultOnlyFixture()},
		{Name: "domain-default-expr-only", Schema: domainDefaultExprOnlyFixture()},
		{Name: "trigger-body-only", Schema: triggerBodyOnlyFixture()},
		{Name: "trigger-foreach-statement", Schema: triggerForEachStatementFixture()},
		{Name: "trigger-transition-tables", Schema: triggerTransitionTablesFixture()},
		{Name: "function-procedure", Schema: functionProcedureFixture()},
		{Name: "partition-expression", Schema: partitionExpressionFixture()},
		{Name: "sequence-scoped", Schema: sequenceScopedFixture()},
		{Name: "constraint-host-struct-only", Schema: constraintHostStructOnlyFixture()},
		{Name: "index-host-struct-only", Schema: indexHostStructOnlyFixture()},
		{Name: "rls-host-struct-only", Schema: rlsHostStructOnlyFixture()},
		{Name: "rls-host-table-only", Schema: rlsHostTableOnlyFixture()},
		{Name: "constraint-host-struct-two-tables", Schema: constraintHostStructTwoTablesFixture()},
		{Name: "index-host-struct-two-tables", Schema: indexHostStructTwoTablesFixture()},
		{Name: "rls-host-struct-two-tables", Schema: rlsHostStructTwoTablesFixture()},
	})
}

// twoNamedTables is two tables, so a host spelling that is ablated cannot be
// answered by "there is only one table it could mean".
func twoNamedTables() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "A", Name: "a"},
			{StructName: "B", Name: "b"},
		},
		Fields: []schemamodel.Field{
			{StructName: "A", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "B", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
		},
	}
}

func constraintHostStructTwoTablesFixture() schemamodel.Database {
	db := twoNamedTables()
	db.Constraints = []schemamodel.Constraint{{
		StructName: "B", Name: "b_id_positive", Type: "CHECK", CheckExpression: "id > 0",
	}}
	return db
}

func indexHostStructTwoTablesFixture() schemamodel.Database {
	db := twoNamedTables()
	db.Indexes = []schemamodel.Index{{
		StructName: "B", Name: "idx_b_id", Fields: []string{"id"},
	}}
	return db
}

func rlsHostStructTwoTablesFixture() schemamodel.Database {
	db := twoNamedTables()
	db.Roles = []schemamodel.Role{{Name: "app_reader"}}
	db.RLSEnabledTables = []schemamodel.RLSEnabledTable{{StructName: "B", Dialects: []string{"sqlserver"}}}
	db.RLSPolicies = []schemamodel.RLSPolicy{{
		StructName: "B", Name: "b_read", PolicyFor: "SELECT",
		ToRoles: "app_reader", UsingExpression: "true", Dialects: []string{"sqlserver"},
	}}
	return db
}

// constraintHostStructOnlyFixture names the host with StructName alone.
func constraintHostStructOnlyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Constraints = []schemamodel.Constraint{{
		StructName: "T", Name: "t_id_positive", Type: "CHECK", CheckExpression: "id > 0",
	}}
	return db
}

func indexHostStructOnlyFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", Fields: []string{"s"},
	}}
	return db
}

func rlsHostStructOnlyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{Name: "app_reader"}}
	db.RLSEnabledTables = []schemamodel.RLSEnabledTable{{StructName: "T", Dialects: []string{"sqlserver"}}}
	db.RLSPolicies = []schemamodel.RLSPolicy{{
		StructName: "T", Name: "t_read", PolicyFor: "SELECT",
		ToRoles: "app_reader", UsingExpression: "true", Dialects: []string{"sqlserver"},
	}}
	return db
}

func rlsHostTableOnlyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{Name: "app_reader"}}
	db.RLSEnabledTables = []schemamodel.RLSEnabledTable{{Table: "t", Dialects: []string{"sqlserver"}}}
	db.RLSPolicies = []schemamodel.RLSPolicy{{
		Table: "t", Name: "t_read", PolicyFor: "SELECT",
		ToRoles: "app_reader", UsingExpression: "true", Dialects: []string{"sqlserver"},
	}}
	return db
}

func columnDefaultEmptyFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "A", Name: "a", Type: "VARCHAR(16)", Nullable: true,
			Default: "", DefaultSet: true,
		},
	)
}

func columnDefaultExprOnlyFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "B", Name: "b", Type: "INTEGER", Nullable: true,
			DefaultExpr: "1",
		},
	)
}

func columnUniqueExprFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true,
			Unique: true, UniqueExpr: "lower(s)",
		},
	)
}

func foreignKeyDeferrableOnlyFixture() schemamodel.Database {
	db := twoTables()
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Child", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
		Foreign: "parents(id)", Deferrable: true,
	})
	return db
}

func foreignKeyInitiallyOnlyFixture() schemamodel.Database {
	db := twoTables()
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Child", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
		Foreign: "parents(id)", Initially: "deferred",
	})
	return db
}

func constraintDeferrableOnlyFixture() schemamodel.Database {
	db := twoTables()
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Child", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
	})
	db.Constraints = []schemamodel.Constraint{{
		StructName: "Child", Table: "children", Name: "fk_children_parent", Type: "FOREIGN KEY",
		Columns: []string{"parent_id"}, ForeignTable: "parents", ForeignColumn: "id",
		Deferrable: true,
	}}
	return db
}

func constraintInitiallyOnlyFixture() schemamodel.Database {
	db := twoTables()
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Child", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
	})
	db.Constraints = []schemamodel.Constraint{{
		StructName: "Child", Table: "children", Name: "fk_children_parent", Type: "FOREIGN KEY",
		Columns: []string{"parent_id"}, ForeignTable: "parents", ForeignColumn: "id",
		Initially: "DEFERRED",
	}}
	return db
}

// fieldCheckNotEnforcedFixture declares a column CHECK the server does not
// check, the one clause of its kind in the fixture (stokaro/ptah#3853).
func fieldCheckNotEnforcedFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"}, schemamodel.Field{
		StructName: "T", FieldName: "N", Name: "n", Type: "INTEGER", Nullable: true,
		Check: "n > 0", CheckNotEnforced: true,
	})
}

func foreignKeyMatchFixture() schemamodel.Database {
	db := twoTables()
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Child", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
		Foreign: "parents(id)", ForeignKeyMatch: "FULL",
	})
	return db
}

func foreignKeyNotEnforcedFixture() schemamodel.Database {
	db := twoTables()
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Child", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
		Foreign: "parents(id)", ForeignKeyNotEnforced: true,
	})
	return db
}

func constraintCheckNotEnforcedFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Constraints = []schemamodel.Constraint{{
		StructName: "T", Table: "t", Name: "t_id_positive", Type: "CHECK", CheckExpression: "id > 0",
		NotEnforced: true,
	}}
	return db
}

func constraintForeignKeyMatchFixture() schemamodel.Database {
	db := twoTables()
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Child", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
	})
	db.Constraints = []schemamodel.Constraint{{
		StructName: "Child", Table: "children", Name: "fk_children_parent", Type: "FOREIGN KEY",
		Columns: []string{"parent_id"}, ForeignTable: "parents", ForeignColumn: "id",
		Match: "FULL",
	}}
	return db
}

// constraintCheckNotValidFixture is a CHECK the table may hold NOT VALID. The
// PostgreSQL family adds it after the table, where the clause is kept.
func constraintCheckNotValidFixture() schemamodel.Database {
	db := twoTables()
	db.Constraints = []schemamodel.Constraint{{
		StructName: "Parent", Table: "parents", Name: "parents_id_positive", Type: "CHECK",
		CheckExpression: "id > 0", NotValid: true,
	}}
	return db
}

// constraintHostTableOnlyFixture names the constraint's host with Table and
// leaves StructName empty, so ablating Table is not answered by the other
// spelling.
func constraintHostTableOnlyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Constraints = []schemamodel.Constraint{{
		Table: "t", Name: "t_id_positive", Type: "CHECK", CheckExpression: "id > 0",
	}}
	return db
}

// indexHostTableOnlyFixture is the same shape for an index.
func indexHostTableOnlyFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
	}}
	return db
}

func domainDefaultOnlyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Domains = []schemamodel.Domain{{
		StructName: "D", Name: "positive", BaseType: "INTEGER", Default: "1",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func domainDefaultExprOnlyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Domains = []schemamodel.Domain{{
		StructName: "D", Name: "positive", BaseType: "INTEGER", DefaultExpr: "1 + 1",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func triggerBodyOnlyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Triggers = []schemamodel.Trigger{{
		StructName: "TR", Name: "t_touch", Table: "t", Timing: "BEFORE", Event: "UPDATE",
		ForEach: "ROW", Body: "SET NEW.id = NEW.id;",
	}}
	return db
}

func triggerForEachStatementFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Functions = []schemamodel.Function{{
		StructName: "F", Name: "touch", Returns: "trigger", Language: "plpgsql",
		Body: "BEGIN RETURN NEW; END;",
	}}
	db.Triggers = []schemamodel.Trigger{{
		StructName: "TR", Name: "t_touch", Table: "t", Timing: "AFTER", Event: "UPDATE",
		ForEach: "STATEMENT", ExecuteFunction: "touch()",
	}}
	return db
}

func functionProcedureFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Functions = []schemamodel.Function{{
		StructName: "F", Name: "do_it", Language: "sql", Body: "SELECT 1;", Kind: "procedure",
	}}
	return db
}

func partitionExpressionFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t",
			PrimaryKey: []string{"id"},
			Partition: &schemamodel.PartitionSpec{
				Type:  "HASH",
				Parts: []schemamodel.PartitionPart{{Expr: "(id % 4)"}},
			},
		}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT"},
		},
	}
}

func sequenceScopedFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Sequences = []schemamodel.Sequence{{
		StructName: "S", Name: "order_seq", Dialects: []string{"postgres"},
	}}
	return db
}

func schemaFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t", Schema: "app"})
	db.Schemas = []schemamodel.Schema{{
		Name: "app", Comment: "application", Charset: "utf8mb4", Collate: "utf8mb4_general_ci",
	}}
	return db
}

func columnCoreFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "Label", Name: "label", Type: "VARCHAR(64)", Nullable: true, Comment: "the label"},
	)
}

func columnDefaultFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "A", Name: "a", Type: "VARCHAR(16)", Nullable: true, Default: "x", DefaultSet: true},
		schemamodel.Field{StructName: "T", FieldName: "B", Name: "b", Type: "INTEGER", Nullable: true, DefaultExpr: "1"},
	)
}

func columnCheckFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "N", Name: "n", Type: "INTEGER", Nullable: true,
			Check: "n > 0", CheckName: "t_n_positive",
		},
	)
}

func columnUniqueFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true, Unique: true},
	)
}

func columnNotNullNameFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)",
			NotNullConstraintName: "t_s_nn",
		},
	)
}

func columnIdentityFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "N", Name: "n", Type: "BIGINT", Nullable: true,
			IdentityGeneration: "BY_DEFAULT", IdentityStart: "10", IdentityIncrement: "2",
			IdentityOptions: "CACHE 5",
		},
	)
}

func columnAutoIncFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true, AutoInc: true},
		},
	}
}

func columnGeneratedFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "P", Name: "p", Type: "INTEGER", Nullable: true},
		schemamodel.Field{
			StructName: "T", FieldName: "D", Name: "d", Type: "INTEGER", Nullable: true,
			GeneratedExpression: "p * 2", GeneratedKind: "STORED",
		},
	)
}

func columnEnumFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "S", Name: "s", Type: "ENUM", Nullable: true,
			Enum: []string{"draft", "live"},
		},
	)
}

// columnEnumForeignKeyFixture is what makes [schemamodel.Field.Enum] visible to
// the ablation.
//
// The values themselves reach DDL through [schemamodel.Database.Enums] -- every
// reader fills both, and the renderer emits the type from the enum -- so
// removing the field from the fixture above changes nothing and the field read
// as a gap against stokaro/ptah#2611.
//
// It has a production reader all the same, and this is it: MySQL refuses a
// foreign key between two ENUM columns whose value lists differ, and the refusal
// is the only place the FIELD decides anything. Ablating it makes the two lists
// equal, the refusal stops, and the render answers SQL where it answered an
// error -- which [everyCell] sees, because it keeps a refusal as its own text.
func columnEnumForeignKeyFixture() schemamodel.Database {
	db := twoTables()
	db.Fields = append(db.Fields,
		schemamodel.Field{
			StructName: "Parent", FieldName: "State", Name: "state", Type: "ENUM",
			Nullable: true, Enum: []string{"draft", "live"},
		},
		schemamodel.Field{
			StructName: "Child", FieldName: "State", Name: "state", Type: "ENUM",
			Nullable: true, Enum: []string{"draft", "live", "archived"},
			Foreign: "parents(state)",
		},
	)
	return db
}

// columnMySQLFixture declares a column collation, and the MySQL owner's
// character set and ON UPDATE clause, bound to the MySQL family.
func columnMySQLFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true,
			Collate: "utf8mb4_bin",
			Facets:  must.Must(mysqlschema.WithColumnSettings(schemaext.Facets{}, mysqlschema.ColumnSettings{Charset: "utf8mb4"})),
		},
		schemamodel.Field{
			StructName: "T", FieldName: "U", Name: "u", Type: "TIMESTAMP", Nullable: true,
			Facets: must.Must(mysqlschema.WithColumnSettings(schemaext.Facets{}, mysqlschema.ColumnSettings{OnUpdate: "CURRENT_TIMESTAMP"})),
		},
	)
}

func columnDeclaredTextFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(80)", Nullable: true,
			TypeIsDeclaredText: true,
		},
	)
}

func columnRawTypeFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "S", Name: "s", Type: "TEXT", Nullable: true,
			TypeRawSQL: true,
		},
	)
}

func columnAPIFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true,
			APIName: "title", APIType: "TEXT", APIExpose: "read-write",
			APINames: schemamodel.TargetNames{GraphQL: "titleGQL", OpenAPI: "titleOA", Protobuf: "titlePB"},
		},
	)
}

func columnOverrideFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true,
			Overrides: map[string]map[string]string{"mysql": {"type": "TEXT"}},
		},
	)
}

func tableCommentFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t", Comment: "the table"})
}

func tableChecksFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t", Checks: []string{"id > 0"}})
}

func tableCustomSQLFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t", CustomSQL: "PARTITION BY RANGE (id)"})
}

func tablePrimaryKeyFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t",
			PrimaryKey: []string{"a", "b"}, PrimaryKeyName: "t_pk",
		}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "A", Name: "a", Type: "VARCHAR(16)"},
			{StructName: "T", FieldName: "B", Name: "b", Type: "BIGINT"},
		},
	}
}

func tablePrimaryKeyIncludeFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t",
			PrimaryKey: []string{"a"}, PrimaryKeyInclude: []string{"b"},
		}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "A", Name: "a", Type: "VARCHAR(16)"},
			{StructName: "T", FieldName: "B", Name: "b", Type: "BIGINT", Nullable: true},
		},
	}
}

// tablePrimaryKeyMethodFixture is a primary key built USING HASH, which MariaDB
// keeps (stokaro/ptah#3853).
func tablePrimaryKeyMethodFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t", PrimaryKey: []string{"a"}, PrimaryKeyMethod: "HASH",
		}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "A", Name: "a", Type: "BIGINT"},
		},
	}
}

// tablePrimaryKeyDeferrableFixture is a primary key that defers its check to
// the end of the transaction (stokaro/ptah#3824).
func tablePrimaryKeyDeferrableFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t",
			PrimaryKey: []string{"a"}, PrimaryKeyDeferrable: true, PrimaryKeyInitially: "deferred",
		}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "A", Name: "a", Type: "BIGINT"},
		},
	}
}

func tablePrimaryKeyPartsFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t",
			PrimaryKey:      []string{"a", "b"},
			PrimaryKeyParts: []schemamodel.PrimaryKeyPart{{Name: "a", Prefix: "8"}, {Name: "b", Desc: true}},
		}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "A", Name: "a", Type: "VARCHAR(16)"},
			{StructName: "T", FieldName: "B", Name: "b", Type: "BIGINT"},
		},
	}
}

func tablePartitionFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t",
			PrimaryKey: []string{"tenant"},
			Partition: &schemamodel.PartitionSpec{
				Type:  "RANGE",
				Parts: []schemamodel.PartitionPart{{Name: "tenant"}},
			},
		}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "Tenant", Name: "tenant", Type: "VARCHAR(16)"},
		},
	}
}

func tableMySQLFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{
		Name: "t", Collate: "utf8mb4_bin",
		Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredTable{Engine: "InnoDB", AutoIncrement: "100", Charset: "utf8mb4"})),
	})
}

// tableEngineFixture declares a table's engine with the common attribute,
// which the MySQL family's owner absorbs.
func tableEngineFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t", Engine: "InnoDB"})
}

func tableSQLiteFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t",
		Facets: must.Must(schemaext.NewFacets(&sqlitetable.DesiredTable{Options: sqlitetable.Options{Strict: true, WithoutRowID: true}}))})
}

func tableUnloggedFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t", Unlogged: true})
}

func tableVirtualFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t",
			VirtualModule: "fts5", VirtualArguments: "body, tokenize='porter'",
		}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "Body", Name: "body", Type: "TEXT", Nullable: true},
		},
	}
}

func tableAPIFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{
		Name: "t", APIName: "Item",
		APINames: schemamodel.TargetNames{GraphQL: "ItemGQL", OpenAPI: "ItemOA", Protobuf: "ItemPB"},
	})
}

func tableOverrideFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{
		Name:      "t",
		Overrides: map[string]map[string]string{"mysql": {"engine": "MyISAM"}},
	})
}

// tableColumnFamiliesFixture sets every setting of a YDB column family, on a
// family holding a column, beside a default family with a setting of its own.
func tableColumnFamiliesFixture() schemamodel.Database {
	families := &ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{
		{Name: "default", Compression: "lz4"},
		{Name: "cold", Data: "hdd", Compression: "lz4", CacheMode: "in_memory", Columns: []string{"payload"}},
	}}
	return oneTable("T", schemamodel.Table{
		Name: "t", Facets: must.Must(schemaext.NewFacets(families)),
	}, schemamodel.Field{StructName: "T", FieldName: "Payload", Name: "payload", Type: "TEXT", Nullable: true})
}

// tableChangefeedFixture sets every option of a YDB changefeed and every
// setting of a consumer, on two consumers since YDB refuses one that is both
// important and limited by an availability period. Its starting partition
// count needs a Uint64 key, which an unsigned BIGINT maps to.
func tableChangefeedFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(ydbschema.DesiredObject("", "t", ydbschema.ChangefeedSpec{
		Name: "updates", Mode: "NEW_IMAGE", Format: "JSON", VirtualTimestamps: true,
		ResolvedTimestamps: "PT10S", InitialScan: true, UserSIDs: true, SchemaChanges: true,
		TopicMinActivePartitions: 2, TopicAutoPartitioning: true, RetentionPeriod: "PT12H",
		Consumers: []ydbtopic.ConsumerSpec{
			{Name: "audit", Important: true, ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw"}},
			{Name: "late", AvailabilityPeriod: "PT1H"},
		},
	})))
	db.FeatureCoverage = must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, nil))
	db.Fields[0].Type = "BIGINT UNSIGNED"
	return db
}

// tableChangefeedDisabledFixture is a changefeed only a reader reports, which
// every target refuses to write: YDB has no statement that disables one.
func tableChangefeedDisabledFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(ydbschema.DesiredObject("", "t", ydbschema.ChangefeedSpec{
		Name: "updates", Mode: "UPDATES", Format: "JSON", Disabled: true,
	})))
	db.FeatureCoverage = must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, nil))
	return db
}

func twoTables() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parents"},
			{StructName: "Child", Name: "children"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Child", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
		},
	}
}

func foreignKeyFieldFixture() schemamodel.Database {
	db := twoTables()
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Child", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
		Foreign: "parents(id)", ForeignKeyName: "fk_children_parent",
		OnDelete: "CASCADE", OnUpdate: "RESTRICT",
	})
	return db
}

// selfReferencingForeignKeyFieldFixture declares a foreign key from a table to
// itself, in the field-level spelling.
//
// The corpus had no such shape and could therefore not exhibit the defect
// #2583 reported -- a self-reference emitted twice, once by the constraint path
// and once by internal/deporder. Every foreign key here pointed at another
// table, so `SelfReferencingForeignKeys` was empty on every fixture and a sweep
// for duplicate emission answered zero for a reason that had nothing to do with
// the invariant.
//
// One table, because a self-reference is what makes the dependency graph name
// the table as its own predecessor, and a second table would give the ordering
// somewhere else to put the edge.
func selfReferencingForeignKeyFieldFixture() schemamodel.Database {
	db := oneTable("Node", schemamodel.Table{Name: "nodes"})
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Node", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
		Foreign: "nodes(id)", ForeignKeyName: "fk_nodes_parent", OnDelete: "CASCADE",
	})
	return db
}

// selfReferencingForeignKeyConstraintFixture is the same reference in the
// table-level spelling.
//
// Both spellings are in the corpus because they reach the renderer through
// different paths: a field's Foreign is read by analyzeFieldForeignKeys, and a
// Constraint whose Table equals its ForeignTable by the constraint path. A
// duplicate emission that only one of them produced would be invisible to a
// corpus carrying the other.
func selfReferencingForeignKeyConstraintFixture() schemamodel.Database {
	db := oneTable("Node", schemamodel.Table{Name: "nodes"})
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Node", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
	})
	db.Constraints = []schemamodel.Constraint{{
		StructName: "Node", Table: "nodes", Name: "fk_nodes_parent", Type: "FOREIGN KEY",
		Columns: []string{"parent_id"}, ForeignTable: "nodes", ForeignColumn: "id",
	}}
	return db
}

func foreignKeyDeferrableFixture() schemamodel.Database {
	db := twoTables()
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Child", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
		Foreign: "parents(id)", Deferrable: true, Initially: "deferred",
	})
	return db
}

func foreignKeyTableFixture() schemamodel.Database {
	db := twoTables()
	db.Fields = append(db.Fields, schemamodel.Field{
		StructName: "Child", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true,
	})
	db.Constraints = []schemamodel.Constraint{{
		StructName: "Child", Table: "children", Name: "fk_children_parent", Type: "FOREIGN KEY",
		Columns: []string{"parent_id"}, ForeignTable: "parents", ForeignColumn: "id",
		OnDelete: "CASCADE", OnUpdate: "RESTRICT", Comment: "the link",
		Deferrable: true, Initially: "DEFERRED",
	}}
	return db
}

func foreignKeyCompositeFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parents", PrimaryKey: []string{"tenant", "id"}},
			{StructName: "Child", Name: "children"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", FieldName: "Tenant", Name: "tenant", Type: "VARCHAR(16)"},
			{StructName: "Parent", FieldName: "ID", Name: "id", Type: "BIGINT"},
			{StructName: "Child", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Child", FieldName: "Tenant", Name: "tenant", Type: "VARCHAR(16)", Nullable: true},
			{StructName: "Child", FieldName: "ParentID", Name: "parent_id", Type: "BIGINT", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Child", Table: "children", Name: "fk_children_parent", Type: "FOREIGN KEY",
			Columns: []string{"tenant", "parent_id"}, ForeignTable: "parents",
			ForeignColumns: []string{"tenant", "id"},
		}},
	}
}

// constraintDeleteColumnListFixture limits ON DELETE SET NULL to one column of
// a composite key, the PostgreSQL 15 form. Both columns stay nullable, so the
// ablated declaration is a valid key that clears both rather than a refusal:
// a refusal would answer every ablation the same way.
func constraintDeleteColumnListFixture() schemamodel.Database {
	db := foreignKeyCompositeFixture()
	db.Constraints[0].OnDelete = "SET NULL"
	db.Constraints[0].OnDeleteColumns = []string{"parent_id"}
	return db
}

func constraintCheckFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Constraints = []schemamodel.Constraint{{
		StructName: "T", Table: "t", Name: "t_id_positive", Type: "CHECK",
		CheckExpression: "id > 0", Comment: "positive",
	}}
	return db
}

func constraintUniqueFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true})
	db.Constraints = []schemamodel.Constraint{{
		StructName: "T", Table: "t", Name: "t_s_uq", Type: "UNIQUE", Columns: []string{"s"},
	}}
	return db
}

func constraintUniqueIncludeFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true},
		schemamodel.Field{StructName: "T", FieldName: "P", Name: "p", Type: "INTEGER", Nullable: true})
	db.Constraints = []schemamodel.Constraint{{
		StructName: "T", Table: "t", Name: "t_s_uq", Type: "UNIQUE", Columns: []string{"s"},
		IncludeColumns: []string{"p"}, NullsDistinct: new(false),
	}}
	return db
}

func constraintPrimaryKeyFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "A", Name: "a", Type: "BIGINT"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "T", Table: "t", Name: "t_pk", Type: "PRIMARY KEY", Columns: []string{"a"},
		}},
	}
}

func constraintExcludeFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true})
	db.Extensions = []schemamodel.Extension{{Name: "btree_gist"}}
	db.Constraints = []schemamodel.Constraint{{
		StructName: "T", Table: "t", Name: "t_excl", Type: "EXCLUDE",
		ExcludeElements: "s WITH =", UsingMethod: "gist", WhereCondition: "s IS NOT NULL",
		RequiresExtensions: []string{"btree_gist"},
	}}
	return db
}

func indexedTable() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true},
		schemamodel.Field{StructName: "T", FieldName: "P", Name: "p", Type: "INTEGER", Nullable: true},
	)
}

func indexBasicFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Unique: true, Comment: "lookup",
	}}
	return db
}

func indexPartialFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Condition: "s IS NOT NULL",
	}}
	return db
}

// indexNullsNotDistinctFixture carries the NULLS [NOT] DISTINCT clause on its
// own, and on a UNIQUE index, which is the only index PostgreSQL accepts it
// on. Riding along on the partial-index fixture hides a second field: where the
// renderer refuses the clause on a target that cannot spell it
// (stokaro/ptah#2820), the refusal takes the whole statement with it and the
// partial predicate stops being observed anywhere.
func indexNullsNotDistinctFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Unique: true, NullsDistinct: new(false),
	}}
	return db
}

func indexIncludeFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		IncludeColumns: []string{"p"},
	}}
	return db
}

func indexPartsFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_parts", TableName: "t",
		Parts: []schemamodel.IndexPart{
			{Name: "s", Desc: true, NullsOrder: "LAST", Operator: "text_pattern_ops", Prefix: "16"},
			{Expr: "lower(s)"},
		},
	}}
	return db
}

func indexStorageFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		StorageParams: map[string]string{"fillfactor": "70"},
	}}
	return db
}

// indexPartitioningFixture sets every partitioning setting of a YDB global
// index but the switch that turns splitting by size off, which a partition
// size cannot share an index with; indexPartitioningUnsplitFixture sets that.
func indexPartitioningFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Facets: indexSettings(ydbschema.IndexPartitioning{
			PartitionSizeMB: 512, ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "PER_AZ:1",
		}),
	}}
	return db
}

func indexPartitioningUnsplitFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Facets: indexSettings(ydbschema.IndexPartitioning{BySize: new(false)}),
	}}
	return db
}

// indexVectorFixture declares a YDB vector index with settings over a vector
// column of the dimension the settings name.
func indexVectorFixture(settings *ydbschema.DesiredVectorIndex) schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "Emb", Name: "emb", Type: "vector(3)", Nullable: true},
	)
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_emb", TableName: "t", Fields: []string{"emb"}, Type: "vector_kmeans_tree",
		Facets: must.Must(schemaext.NewFacets(settings)),
	}}
	return db
}

// tablePartitioningFixture sets every setting of a YDB row table but the
// switch that turns splitting by size off, which a partition size cannot share
// a table with, and the two starting layouts, which cannot share a table with
// each other; the three fixtures after it set those.
func tablePartitioningFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t", Facets: tableSettings(ydbschema.TablePartitioning{
		PartitionSizeMB: 512, ByLoad: new(true), MinPartitions: 3, MaxPartitions: 9, ReadReplicas: "PER_AZ:1",
		KeyBloomFilter: new(true),
	})})
}

func tablePartitioningUnsplitFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t", Facets: tableSettings(ydbschema.TablePartitioning{BySize: new(false)})})
}

// tableUniformPartitionsFixture keys the table on an unsigned column, the
// only kind whose range YDB splits evenly.
func tableUniformPartitionsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t", Facets: tableSettings(ydbschema.TablePartitioning{UniformPartitions: 4})})
	db.Fields[0].Type = "BIGINT UNSIGNED"
	return db
}

func tablePartitionAtKeysFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t",
		Facets: tableSettings(ydbschema.TablePartitioning{PartitionAtKeys: [][]string{{"10"}, {"20"}}})})
}

// tableSettings is a YDB row table's settings as the YDB owner's facet.
func tableSettings(settings ydbschema.TablePartitioning) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&ydbschema.DesiredTablePartitioning{TablePartitioning: settings}))
}

// indexSettings is a YDB global index's settings as the YDB owner's facet.
func indexSettings(settings ydbschema.IndexPartitioning) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&ydbschema.DesiredIndexPartitioning{IndexPartitioning: settings}))
}

func indexConcurrentFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Concurrently: true,
	}}
	return db
}

// indexClickHouseFixture declares a skipping index's settings as ClickHouse
// source properties, so the census measures that the selected owner decodes
// Overrides into the type and granularity it renders.
func indexClickHouseFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Overrides: map[string]map[string]string{"clickhouse": {"type": "set(100)", "granularity": "4"}},
	}}
	return db
}

// indexMySQLParserFixture declares a FULLTEXT index with the parser the MySQL
// owner holds.
func indexMySQLParserFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"}, Type: "FULLTEXT",
		Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndex{Parser: "ngram"})),
	}}
	return db
}

func indexFullTextFixture() schemamodel.Database {
	db := indexedTable()
	db.Extensions = []schemamodel.Extension{{Name: "pg_trgm"}}
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Type: "GIN", Operator: "gin_trgm_ops",
		RequiresExtensions: []string{"pg_trgm"},
	}}
	return db
}

// indexInvisibleFixture is an index the optimizer does not use, which the
// MySQL family writes as INVISIBLE or IGNORED (stokaro/ptah#3853).
func indexInvisibleFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"}, Invisible: true,
	}}
	return db
}

func enumFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Enums = []schemamodel.Enum{{Name: "mood", Schema: "public", Values: []string{"ok", "bad"}, Comment: "feelings"}}
	return db
}

func domainFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Domains = []schemamodel.Domain{{
		StructName: "D", Name: "positive", Schema: "public", BaseType: "INTEGER",
		NotNull: true, Default: "1", DefaultExpr: "1", Check: "VALUE > 0",
		Comment: "positive int", Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func compositeFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.CompositeTypes = []schemamodel.CompositeType{{
		StructName: "C", Name: "address", Schema: "public", Comment: "postal",
		Fields:   []schemamodel.CompositeField{{Name: "street", Type: "TEXT"}},
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func rangeFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Ranges = []schemamodel.Range{{
		StructName: "R", Name: "floatrange", Schema: "public", Subtype: "float8",
		SubtypeOpClass: "float8_ops", Collation: "C", Canonical: "float8range_canonical",
		SubtypeDiff: "float8mi", Comment: "float range", ClearedAttributes: []string{"collation"},
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func sequenceFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Sequences = []schemamodel.Sequence{{
		StructName: "S", Name: "order_seq", Schema: "public", AsType: "bigint",
		Start: new(int64(5)), Increment: new(int64(2)), MinValue: new(int64(1)), MaxValue: new(int64(999)), Cache: new(int64(4)),
		Cycle: true, IfNotExists: true, OwnedBy: "t.id", Comment: "order numbers",
	}}
	return db
}

func extensionFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Extensions = []schemamodel.Extension{{
		Name: "postgis", Schema: "public", Version: "3.4", IfNotExists: true,
		Comment: "geo", Provides: []string{"geometry"},
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func viewFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Views = []schemamodel.View{{
		StructName: "V", Name: "recent", Comment: "recent rows",
		Body: "SELECT id FROM t", WithCheck: true,
		Attributes: []string{"SCHEMABINDING"},
		Dialects:   []string{"postgres", "cockroachdb", "yugabytedb", "sqlserver"},
	}}
	return db
}

func matViewFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.MaterializedViews = []schemamodel.MaterializedView{{
		StructName: "MV", Name: "daily", Comment: "daily rollup",
		Body: "SELECT id FROM t", Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func functionFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Functions = []schemamodel.Function{{
		StructName: "F", Name: "touch", Parameters: "a integer", Returns: "integer",
		Language: "sql", Security: "DEFINER", Volatility: "STABLE",
		Settings: []string{"search_path = public"}, Body: "SELECT a;",
		Comment: "identity", Kind: "function",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

// functionPlannerPropertiesFixture declares the two properties that decide how
// the planner may use a routine.
//
// It is separate from [functionFixture] rather than an edit to it. Neither
// property is written when the routine states nothing, and that is the shape
// most declarations take, so one fixture carrying both would stop measuring the
// unmarked render.
func functionPlannerPropertiesFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Functions = []schemamodel.Function{{
		StructName: "F", Name: "pure", Parameters: "a integer", Returns: "integer",
		Language: "sql", Volatility: "IMMUTABLE", Body: "SELECT a;",
		Leakproof: true, Parallel: "SAFE", Strict: true, Kind: "function",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func triggerFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Functions = []schemamodel.Function{{
		StructName: "F", Name: "touch", Returns: "trigger", Language: "plpgsql",
		Body: "BEGIN RETURN NEW; END;", Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	db.Triggers = []schemamodel.Trigger{{
		StructName: "TR", Name: "t_touch", Table: "t", Timing: "BEFORE", Event: "UPDATE",
		ForEach: "ROW", ExecuteFunction: "touch()", Body: "BEGIN RETURN NEW; END;",
		When:    "NEW.id IS DISTINCT FROM OLD.id",
		Comment: "touch", Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func triggerTransitionTablesFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Functions = []schemamodel.Function{{
		StructName: "F", Name: "audit", Returns: "trigger", Language: "plpgsql",
		Body: "BEGIN RETURN NULL; END;", Dialects: []string{"postgres"},
	}}
	db.Triggers = []schemamodel.Trigger{{
		StructName: "TR", Name: "t_audit", Table: "t", Timing: "AFTER", Event: "UPDATE",
		ForEach: "STATEMENT", ExecuteFunction: "audit()", OldTable: "old_rows", NewTable: "new_rows",
		Dialects: []string{"postgres"},
	}}
	return db
}

func hypertableFixture() schemamodel.Database {
	hypertable := &tsschema.DesiredHypertable{Column: "at", ChunkInterval: "1 day", IfNotExists: true, Comment: "time series"}
	db := oneTable("T", schemamodel.Table{Name: "t", Facets: must.Must(schemaext.NewFacets(hypertable))},
		schemamodel.Field{StructName: "T", FieldName: "At", Name: "at", Type: "TIMESTAMP", Nullable: true})
	db.Extensions = []schemamodel.Extension{{Name: "timescaledb"}}
	db.FeatureCoverage = must.Must(tsschema.CompleteCoverage(schemaext.Desired))
	return db
}

func continuousAggregateFixture() schemamodel.Database {
	hypertable := &tsschema.DesiredHypertable{Column: "at"}
	db := oneTable("T", schemamodel.Table{Name: "t", Facets: must.Must(schemaext.NewFacets(hypertable))},
		schemamodel.Field{StructName: "T", FieldName: "At", Name: "at", Type: "TIMESTAMP", Nullable: true})
	db.Extensions = []schemamodel.Extension{{Name: "timescaledb"}}
	db.FeatureObjects = must.Must(schemaext.NewObjects(tsschema.DesiredContinuousAggregateObject("public", "t_hourly",
		tsschema.DesiredContinuousAggregate{StructName: "CA", Body: "SELECT id FROM t", MaterializedOnly: new(true), Comment: "hourly"})))
	db.FeatureCoverage = must.Must(tsschema.CompleteCoverage(schemaext.Desired))
	return db
}

func synonymFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(synonym.DeclaredObject(synonym.DesiredSynonym{
		StructName: "SY", Comment: "alias", Synonym: synonym.Synonym{Name: "tt", Schema: "dbo", Target: "dbo.t"},
	})))
	db.FeatureCoverage = must.Must(synonym.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}

// topicFixture declares a YDB topic that sets every setting and two consumers
// that set every consumer setting between them: important and an
// availability period are one consumer each, because YDB refuses a consumer
// holding both.
func topicFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(ydbtopic.DesiredObject("app", "events", "TO", ydbtopic.Spec{
		MinActivePartitions: 2, MaxActivePartitions: 6, AutoPartitioningStrategy: "scale_up",
		AutoPartitioningUpUtilizationPercent: 70, AutoPartitioningDownUtilizationPercent: 10,
		AutoPartitioningStabilizationWindow: "PT2M", RetentionPeriod: "PT36H",
		PartitionWriteSpeedBytesPerSecond: 2097152, PartitionWriteBurstBytes: 3145728,
		SupportedCodecs: []string{"raw", "gzip"},
		Consumers: []ydbtopic.ConsumerSpec{
			{Name: "billing", Important: true},
			{Name: "audit", ReadFrom: "2026-01-01T00:00:00Z", SupportedCodecs: []string{"raw", "gzip"},
				AvailabilityPeriod: "PT2H"},
		},
	})))
	db.FeatureCoverage = must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}

// resourcePoolFixture declares a YDB resource pool that sets every setting,
// and a classifier that sends a member's queries to it.
func resourcePoolFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(
		ydbworkload.DesiredPoolObject("reporting", "RP", ydbworkload.PoolSpec{
			ConcurrentQueryLimit: new(int32(10)), QueueSize: new(int32(20)),
			DatabaseLoadCPUThreshold: new(80.5), QueryMemoryLimitPercentPerNode: new(25.0),
			QueryCPULimitPercentPerNode: new(30.0), TotalCPULimitPercentPerNode: new(70.0),
			ResourceWeight: new(5.0),
		}),
		ydbworkload.DesiredClassifierObject("reporting_group", "RP", ydbworkload.ClassifierSpec{ResourcePool: "reporting", MemberName: "reporters", Rank: 100}),
	))
	pools := must.Must(ydbworkload.Coverage(ydbworkload.PoolKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	classifiers := must.Must(ydbworkload.Coverage(ydbworkload.ClassifierKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	db.FeatureCoverage = must.Must(pools.Combine(classifiers))
	return db
}

// asyncReplicationFixture declares a YDB async replication that sets every
// setting a replication of a user with a password secret takes, and two items,
// one naming its source by an absolute path.
func asyncReplicationFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(ydbreplication.DesiredReplicationObject("app", "mirror", "AR", mirrorReplicationSpec())))
	db.FeatureCoverage = must.Must(ydbreplication.ReplicationCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}

func mirrorReplicationSpec() ydbreplication.ReplicationSpec {
	return ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{
			ConnectionString:   "grpcs://primary.example.com:2135/?database=/prod",
			User:               "replicator",
			PasswordSecretPath: "secrets/replicator",
		},
		Items: []ydbreplication.Item{
			{Source: "accounts", Target: "replica/accounts"},
			{Source: "/prod/ledger", Target: "replica/ledger"},
		},
		ConsistencyLevel: "global",
		CommitInterval:   "PT30S",
	}
}

// asyncReplicationTokenFixture declares the two token credentials, one
// replication each, since a connection takes one credential.
func asyncReplicationTokenFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	byName, byPath := tokenReplicationSpecs()
	db.FeatureObjects = must.Must(schemaext.NewObjects(
		ydbreplication.DesiredReplicationObject("", "by_name", "AN", byName),
		ydbreplication.DesiredReplicationObject("", "by_path", "AP", byPath),
	))
	db.FeatureCoverage = must.Must(ydbreplication.ReplicationCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}

func tokenReplicationSpecs() (byName, byPath ydbreplication.ReplicationSpec) {
	connection := "grpc://primary.example.com:2136/?database=/prod"
	byName = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: connection, TokenSecretName: "token"},
		Items:      []ydbreplication.Item{{Source: "orders", Target: "orders_by_name"}},
	}
	byPath = ydbreplication.ReplicationSpec{
		Connection: ydbreplication.Connection{ConnectionString: connection, TokenSecretPath: "secrets/token"},
		Items:      []ydbreplication.Item{{Source: "orders", Target: "orders_by_path"}},
	}
	return byName, byPath
}

// transferFixture declares a YDB transfer of a topic in another database into
// a declared table, setting every setting a transfer takes, its credential a
// user with a password secret named by name.
func transferFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(ydbreplication.DesiredTransferObject("app", "ingest", "TF", ingestTransferSpec())))
	db.FeatureCoverage = must.Must(ydbreplication.TransferCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}

func ingestTransferSpec() ydbreplication.TransferSpec {
	return ydbreplication.TransferSpec{
		Connection: ydbreplication.Connection{
			ConnectionString:   "grpc://primary.example.com:2136/?database=/prod",
			User:               "reader",
			PasswordSecretName: "reader_password",
		},
		Source:         "events",
		Target:         "t",
		Lambda:         "($msg) -> { return [<| id: $msg._offset |>]; }",
		Consumer:       "ingest",
		BatchSizeBytes: 1048576,
		FlushInterval:  "PT10S",
	}
}

func ownedCoordinationNodeFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: ydbcoordination.Ref("app", "locks"),
		Value: &ydbcoordination.Desired{Spec: ydbcoordination.Spec{
			SelfCheckPeriodMillis: 2000, SessionGracePeriodMillis: 15000,
			ReadConsistencyMode: "strict", AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
		}},
	}))
	db.FeatureCoverage = must.Must(ydbcoordination.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}

// secretFixture declares a YDB secret in a directory, naming the variable its
// value comes from.
func secretFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(ydbsecret.DesiredObject("ext", "pg_password", "SE", "PTAH_SECRET_PG_PASSWORD")))
	db.FeatureCoverage = must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}

// externalObjectsFixture declares a YDB external data source whose password a
// secret holds, an object storage source, and an external table over the
// second with every part a declaration writes.
func externalObjectsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(
		ydbexternal.DesiredSourceObject("ext", "warehouse", "ES", ydbexternal.DataSource{SourceType: "PostgreSQL", Location: "pg:5432",
			AuthMethod: "BASIC", Options: map[string]string{"DATABASE_NAME": "app", "LOGIN": "reader",
				"PASSWORD_SECRET_PATH": "ext/pg_password"}}),
		ydbexternal.DesiredSourceObject("ext", "bucket", "ES", ydbexternal.DataSource{SourceType: "ObjectStorage",
			Location: "https://s3.example.test/b/", AuthMethod: "NONE"}),
		ydbexternal.DesiredTableObject("ext", "events", "ET", ydbexternal.Table{DataSource: "ext/bucket", Location: "events/",
			Columns: []ydbexternal.Column{{Name: "id", Type: "Int64", NotNull: true}},
			Options: map[string]string{"FORMAT": "json_each_row"}}),
	))
	sources := must.Must(ydbexternal.SourceCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	tables := must.Must(ydbexternal.TableCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	db.FeatureCoverage = must.Must(sources.Combine(tables))
	return db
}

func extendedPropertyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(mssqlproperty.DeclaredObject(mssqlproperty.DesiredProperty{
		StructName: "XP", Comment: "docs",
		Property: mssqlproperty.Property{Name: "ptah_note", Schema: "dbo", Table: "t", Column: "id", Value: "identifier"},
	})))
	db.FeatureCoverage = must.Must(mssqlproperty.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}

func roleFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{
		StructName: "RO", Name: "app_reader", Login: true, Password: "s3cret",
		Superuser: true, CreateDB: true, CreateRole: true, Inherit: true, Replication: true,
		Comment: "reader", Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

// ydbGroupMembershipFixture declares a YDB group and a user that is a member
// of it, which only a target with groups and memberships renders.
func ydbGroupMembershipFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{
		{StructName: "RO", Name: "readers", Group: true, Inherit: true, Dialects: []string{"ydb"}},
		{StructName: "RO", Name: "app", Login: true, Inherit: true, MemberOf: []string{"readers"}, Dialects: []string{"ydb"}},
	}
	return db
}

// ydbGrantDatabaseFixture grants a permission on the database itself, which a
// YDB render names by the path of the database a read was made from. A
// declaration is not about one database, so the census renders it without a
// path, and a YDB render refuses the grant.
func ydbGrantDatabaseFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app", Login: true, Inherit: true, Dialects: []string{"ydb"}}}
	db.Grants = []schemamodel.Grant{{
		StructName: "G", Role: "app", Privileges: []string{"CONNECT"}, OnDatabase: true, Dialects: []string{"ydb"},
	}}
	return db
}

func grantTableFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true}}
	db.Grants = []schemamodel.Grant{{
		StructName: "G", Role: "app_reader", Privileges: []string{"SELECT"}, OnTable: "t",
		WithOption: true, GrantedBy: "postgres", Comment: "read",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func grantSchemaFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true}}
	db.Grants = []schemamodel.Grant{{
		StructName: "G", Role: "app_reader", Privileges: []string{"USAGE"}, OnSchema: "public",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func grantSequenceFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true}}
	db.Sequences = []schemamodel.Sequence{{StructName: "S", Name: "order_seq"}}
	db.Grants = []schemamodel.Grant{{
		StructName: "G", Role: "app_reader", Privileges: []string{"USAGE"}, OnSequence: "order_seq",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

// grantRoutineFixture grants on a procedure, so that each part of a routine
// target is measured: the name, the argument types that pick one overload, and
// the kind, whose default is FUNCTION and which a procedure is the only way to
// see.
func grantRoutineFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true}}
	db.Functions = []schemamodel.Function{{
		StructName: "F", Name: "do_it", Parameters: "p uuid", Language: "sql", Body: "SELECT 1;", Kind: "procedure",
	}}
	db.Grants = []schemamodel.Grant{{
		StructName: "G", Role: "app_reader", Privileges: []string{"EXECUTE"},
		OnRoutine: "do_it", RoutineArguments: "uuid", RoutineKind: "PROCEDURE",
		Dialects: []string{"postgres"},
	}}
	return db
}

// grantColumnsFixture limits a privilege to one column of a table. Scoped to
// the PostgreSQL family: the other renderers refuse a column list by name.
func grantColumnsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "Label", Name: "label", Type: "TEXT"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true}}
	db.Grants = []schemamodel.Grant{{
		StructName: "G", Role: "app_reader", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"label"},
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

// revokedGrantFixture asserts a privilege absent.
func revokedGrantFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true}}
	db.RevokedGrants = []schemamodel.Grant{{
		StructName: "RG", Role: "app_reader", Privileges: []string{"INSERT"}, OnTable: "t",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

// defaultPrivilegeBase is what the default-privilege fixtures vary one
// attribute of: a table in a declared schema, and the two roles the statement
// names.
//
// The schema and both roles are declared because the statement names all three.
// Without them the fixture asks for a default privilege in a schema no render
// creates, between roles no render creates, and a target refusing the
// declaration for that reason answers every ablation the same way.
//
// The slices are literals in a fixed order and nothing here is walked out of a
// map: two renders of one fixture have to produce the same bytes, and a corpus
// built from map iteration flakes rather than fails.
func defaultPrivilegeBase() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t", Schema: "app"})
	db.Schemas = []schemamodel.Schema{{Name: "app"}}
	db.Roles = []schemamodel.Role{
		{StructName: "OWNER", Name: "app_owner", Login: true},
		{StructName: "READER", Name: "app_reader", Login: true},
	}
	return db
}

// defaultPrivilegeTablesFixture is the TABLES arm, and the one default-privilege
// fixture carrying a comment.
//
// One object type per fixture rather than four declarations in one, for the
// reason [Fixture] gives: a target that refuses one keyword answers every
// ablation inside that fixture with the same refusal, and the other three
// object types would then be unmeasured while reading as unobservable.
func defaultPrivilegeTablesFixture() schemamodel.Database {
	db := defaultPrivilegeBase()
	db.DefaultPrivileges = []schemamodel.DefaultPrivilege{{
		StructName: "DPTables", Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader", Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
		Comment:  "new tables in app are readable",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

// defaultPrivilegeSequencesFixture is the SEQUENCES arm.
func defaultPrivilegeSequencesFixture() schemamodel.Database {
	db := defaultPrivilegeBase()
	db.DefaultPrivileges = []schemamodel.DefaultPrivilege{{
		StructName: "DPSequences", Grantor: "app_owner", Schema: "app", ObjectType: "SEQUENCES",
		Grantee: "app_reader", Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}},
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

// defaultPrivilegeFunctionsFixture is the FUNCTIONS arm, carrying the privilege
// only a routine takes.
func defaultPrivilegeFunctionsFixture() schemamodel.Database {
	db := defaultPrivilegeBase()
	db.DefaultPrivileges = []schemamodel.DefaultPrivilege{{
		StructName: "DPFunctions", Grantor: "app_owner", Schema: "app", ObjectType: "FUNCTIONS",
		Grantee: "app_reader", Privileges: []schemamodel.PrivilegeGrant{{Privilege: "EXECUTE"}},
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

// defaultPrivilegeTypesFixture is the TYPES arm.
func defaultPrivilegeTypesFixture() schemamodel.Database {
	db := defaultPrivilegeBase()
	db.DefaultPrivileges = []schemamodel.DefaultPrivilege{{
		StructName: "DPTypes", Grantor: "app_owner", Schema: "app", ObjectType: "TYPES",
		Grantee: "app_reader", Privileges: []schemamodel.PrivilegeGrant{{Privilege: "USAGE"}},
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

// defaultPrivilegeGrantableFixture carries a grantable privilege beside a plain
// one on one declaration.
//
// Grantability is per privilege, and the pair is what makes that observable: a
// declaration whose privileges all carry the grant option renders the same bytes
// under a renderer that attached WITH GRANT OPTION to the whole statement, so
// ablating PrivilegeGrant.WithOption would report the field read either way.
// With the pair, the two spellings produce different statements.
func defaultPrivilegeGrantableFixture() schemamodel.Database {
	db := defaultPrivilegeBase()
	db.DefaultPrivileges = []schemamodel.DefaultPrivilege{{
		StructName: "DPGrantable", Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
		Grantee: "app_reader",
		Privileges: []schemamodel.PrivilegeGrant{
			{Privilege: "SELECT"},
			{Privilege: "INSERT", WithOption: true},
		},
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

// defaultPrivilegeGlobalFixture is the global default, without a schema, taking
// PUBLIC's built-in EXECUTE on functions away. It is the fixture where Revoked
// renders: a new database starts from the built-in default, so the revoke is a
// statement it needs, where a schema-scoped revoke on a new database takes back
// nothing.
func defaultPrivilegeGlobalFixture() schemamodel.Database {
	db := defaultPrivilegeBase()
	db.DefaultPrivileges = []schemamodel.DefaultPrivilege{{
		StructName: "DPGlobal", Grantor: "app_owner", ObjectType: "FUNCTIONS",
		Grantee: "PUBLIC", Revoked: []string{"EXECUTE"},
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func rlsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader"}}
	db.RLSEnabledTables = []schemamodel.RLSEnabledTable{{
		StructName: "T", Table: "t", Comment: "rls",
		Dialects: []string{"sqlserver"},
	}}
	db.RLSPolicies = []schemamodel.RLSPolicy{{
		StructName: "T", Name: "t_read", Table: "t", PolicyFor: "SELECT",
		ToRoles: "app_reader", UsingExpression: "true", WithCheckExpression: "true",
		Comment: "read policy", Dialects: []string{"sqlserver"},
	}}
	return db
}

// rlsStrengthFixture declares the stronger of each row-level-security pair:
// a table whose owner its policies also bind, and a policy that narrows access
// rather than widening it.
//
// It is separate from [rlsFixture] rather than an edit to it. The weaker
// spelling of each is what most declarations use and what the renderer must
// leave unmarked, so a single fixture carrying only the stronger one would stop
// measuring that.
func rlsStrengthFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader"}}
	db.RLSEnabledTables = []schemamodel.RLSEnabledTable{{
		StructName: "T", Table: "t", Forced: true,
		Dialects: []string{"sqlserver"},
	}}
	db.RLSPolicies = []schemamodel.RLSPolicy{{
		StructName: "T", Name: "t_tenant", Table: "t", PolicyFor: "ALL",
		ToRoles: "app_reader", UsingExpression: "true", Restrictive: true,
		Dialects: []string{"sqlserver"},
	}}
	return db
}

func embeddedJSONFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.EmbeddedFields = []schemamodel.EmbeddedField{{
		StructName: "T", Mode: "json", Name: "meta", Type: "JSONB",
		Nullable: true, Comment: "metadata", EmbeddedTypeName: "Meta",
		Overrides: map[string]map[string]string{"mysql": {"type": "JSON"}},
	}}
	return db
}

func embeddedRelationFixture() schemamodel.Database {
	db := twoTables()
	db.EmbeddedFields = []schemamodel.EmbeddedField{{
		StructName: "Child", Mode: "relation", Field: "parent_id", Ref: "parents(id)",
		Type: "BIGINT", OnDelete: "CASCADE", OnUpdate: "RESTRICT", Nullable: true,
		EmbeddedTypeName: "Parent",
	}}
	return db
}

func embeddedInlineFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.EmbeddedFields = []schemamodel.EmbeddedField{{
		StructName: "T", Mode: "inline", Prefix: "audit_", EmbeddedTypeName: "Audit",
	}}
	db.EmbeddedSources = schemamodel.EmbeddedSources{
		Definitions: []schemamodel.EmbeddedField{{
			StructName: "T", Mode: "inline", Prefix: "audit_", EmbeddedTypeName: "Audit",
		}},
		Fields: []schemamodel.Field{
			{StructName: "Audit", FieldName: "At", Name: "at", Type: "TIMESTAMP", Nullable: true},
		},
	}
	return db
}

func managedDataFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.ManagedData = []schemamodel.ManagedData{{
		StructName: "T", Table: "t", Schema: "public",
		File: "t.yaml", SourceDir: "data", Keys: []string{"id"},
	}}
	return db
}

func coverageFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.NotDescribed = coverage.Set{}.
		WithKind(coverage.Extension).
		With(coverage.Object{
			Kind: coverage.Sequence, Name: "other",
			Provenance: coverage.Observed, Reason: coverage.NotInspected,
		})
	return db
}

// indexKeyBlockSizeFixture isolates the MySQL-family index block-size hint.
func indexKeyBlockSizeFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8})),
	}}
	return db
}

func primaryKeyOptionsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Tables[0].PrimaryKey = []string{"id"}
	db.Tables[0].PrimaryKeyComment, db.Tables[0].PrimaryKeyBlockSize = "lookup", 8
	return db
}

func primaryKeyConstraintOptionsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Constraints = []schemamodel.Constraint{{StructName: "T", Table: "t", Name: "PRIMARY", Type: "PRIMARY KEY", Columns: []string{"id"}, Comment: "lookup", KeyBlockSize: 8}}
	return db
}

func streamingQueryFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.FeatureObjects = must.Must(schemaext.NewObjects(ydbstreaming.DesiredObject("streams", "copy", "Streaming",
		ydbstreaming.Spec{Text: "INSERT INTO output SELECT * FROM input;", Run: new(false), ResourcePool: "reporting"}, true)))
	db.FeatureCoverage = must.Must(ydbstreaming.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return db
}

func tableColumnStoreFixture() schemamodel.Database {
	store := &ydbschema.DesiredColumnStore{ColumnStore: ydbschema.ColumnStore{HashColumns: []string{"id"}, Partitions: 8,
		TTL: &ydbschema.TieredTTL{Column: "id", Unit: "SECONDS", Tiers: []ydbschema.TTLTier{{Interval: "P1D", ExternalSource: "/local/archive"}, {Interval: "P7D"}}}}}
	db := oneTable("T", schemamodel.Table{Name: "t", Facets: must.Must(schemaext.NewFacets(store))})
	db.Fields[0].Type = "BIGINT UNSIGNED"
	return db
}

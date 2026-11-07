package schemacensus

import (
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	platformidentifier "ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/postgres/pgroutine"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/synonym"
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

	// populated is every field path the schema declares, from
	// [PopulatedFields]; nil on a fixture [Fixtures] did not build.
	populated map[string]bool
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
	fixtures := corpus()
	for index := range fixtures {
		fixtures[index].populated = PopulatedFields(fixtures[index].Schema)
	}
	return fixtures
}

// declares reports whether the fixture's schema holds a non-zero instance of
// the field named by path: [Populated], answered from one walk made when the
// corpus was built.
func (f Fixture) declares(path string) bool {
	if f.populated == nil {
		return Populated(f.Schema, path)
	}
	return f.populated[path]
}

// corpus is the fixtures themselves.
func corpus() []Fixture {
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
		{Name: "table-partition-member", Schema: tablePartitionMemberFixture()},
		{Name: "table-mysql", Schema: tableMySQLFixture()},
		{Name: "table-engine", Schema: tableEngineFixture()},
		{Name: "table-sqlite", Schema: tableSQLiteFixture()},
		{Name: "table-unlogged", Schema: tableUnloggedFixture()},
		{Name: "table-virtual", Schema: tableVirtualFixture()},
		{Name: "table-api", Schema: tableAPIFixture()},
		{Name: "table-override", Schema: tableOverrideFixture()},
		{Name: "table-rowttl", Schema: tableRowTTLFixture()},
		{Name: "fk-field", Schema: foreignKeyFieldFixture()},
		{Name: "fk-field-deferrable", Schema: foreignKeyDeferrableFixture()},
		{Name: "fk-table", Schema: foreignKeyTableFixture()},
		{Name: "fk-table-composite", Schema: foreignKeyCompositeFixture()},
		{Name: "constraint-check", Schema: constraintCheckFixture()},
		{Name: "constraint-unique", Schema: constraintUniqueFixture()},
		{Name: "constraint-unique-include", Schema: constraintUniqueIncludeFixture()},
		{Name: "constraint-pk", Schema: constraintPrimaryKeyFixture()},
		{Name: "constraint-pk-method", Schema: constraintPrimaryKeyMethodFixture()},
		{Name: "constraint-exclude", Schema: constraintExcludeFixture("deferred")},
		{Name: "constraint-exclude-deferrable", Schema: constraintExcludeFixture("")},
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
		{Name: "index-hash", Schema: indexHashFixture()},
		{Name: "index-invisible", Schema: indexInvisibleFixture()},
		{Name: "index-key-block-size", Schema: indexKeyBlockSizeFixture()},
		{Name: "enum", Schema: enumFixture()},
		{Name: "sequence", Schema: sequenceFixture()},
		{Name: "view", Schema: viewFixture()},
		{Name: "view-attributes", Schema: viewAttributesFixture()},
		{Name: "view-options", Schema: viewOptionsFixture()},
		{Name: "view-local-check", Schema: viewLocalCheckFixture()},
		{Name: "matview", Schema: matViewFixture()},
		{Name: "matview-options", Schema: matViewOptionsFixture()},
		{Name: "matview-refresh", Schema: matViewRefreshFixture()},
		{Name: "matview-refresh-after", Schema: matViewRefreshAfterFixture()},
		{Name: "table-row-policy", Schema: tableRowPolicyFixture()},
		{Name: "table-row-policy-all-except", Schema: tableRowPolicyAllExceptFixture()},
		{Name: "function", Schema: functionFixture()},
		{Name: "function-planner-properties", Schema: functionPlannerPropertiesFixture()},
		{Name: "trigger", Schema: triggerFixture()},
		{Name: "hypertable", Schema: hypertableFixture()},
		{Name: "continuous-aggregate", Schema: continuousAggregateFixture()},
		{Name: "synonym", Schema: synonymFixture()},
		{Name: "security-policy", Schema: securityPolicyFixture()},
		{Name: "routine-options", Schema: routineOptionsFixture()},
		{Name: "pgpolicy-policy", Schema: tablePolicyFixture()},
		{Name: "pgpolicy-policy-declared", Schema: tableDeclaredPolicyFixture()},
		{Name: "pgpolicy-table-state", Schema: tableRowSecurityFixture()},
		{Name: "pgextension-extension", Schema: ownedExtensionFixture()},
		{Name: "pgtype-types", Schema: ownedTypeFixture()},
		{Name: "oratype-types", Schema: ownedOracleTypeFixture()},
		{Name: "extended-property", Schema: extendedPropertyFixture()},
		{Name: "role", Schema: roleFixture()},
		{Name: "role-cockroachdb", Schema: cockroachRoleFixture()},
		{Name: "role-group", Schema: roleGroupFixture()},
		{Name: "role-membership", Schema: roleMembershipFixture()},
		{Name: "grant-database", Schema: grantDatabaseFixture()},
		{Name: "grant-object", Schema: grantObjectFixture()},
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
		{Name: "trigger-body-only", Schema: triggerBodyOnlyFixture()},
		{Name: "trigger-foreach-statement", Schema: triggerForEachStatementFixture()},
		{Name: "trigger-transition-tables", Schema: triggerTransitionTablesFixture()},
		{Name: "function-procedure", Schema: functionProcedureFixture()},
		{Name: "partition-expression", Schema: partitionExpressionFixture()},
		{Name: "partition-modifiers", Schema: partitionModifiersFixture()},
		{Name: "sequence-scoped", Schema: sequenceScopedFixture()},
		{Name: "constraint-host-struct-only", Schema: constraintHostStructOnlyFixture()},
		{Name: "index-host-struct-only", Schema: indexHostStructOnlyFixture()},
		{Name: "rls-host-struct-only", Schema: rlsHostStructOnlyFixture()},
		{Name: "rls-host-table-only", Schema: rlsHostTableOnlyFixture()},
		{Name: "constraint-host-struct-two-tables", Schema: constraintHostStructTwoTablesFixture()},
		{Name: "index-host-struct-two-tables", Schema: indexHostStructTwoTablesFixture()},
		{Name: "index-host-table-two-tables", Schema: indexHostTableTwoTablesFixture()},
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
	db := twoIndexedTables()
	db.Indexes = []schemamodel.Index{{
		StructName: "B", Name: "idx_b_s", Fields: []string{"s"},
	}}
	return db
}

// indexHostTableTwoTablesFixture names the host by table name beside a second
// table, so the round trip can move the index to the other one.
func indexHostTableTwoTablesFixture() schemamodel.Database {
	db := twoIndexedTables()
	db.Indexes = []schemamodel.Index{{
		TableName: "b", Name: "idx_b_s", Fields: []string{"s"},
	}}
	return db
}

// twoIndexedTables is [twoNamedTables] with a column outside the key on each
// table, which an index reads, and moving the index to the other table needs
// it there too.
func twoIndexedTables() schemamodel.Database {
	db := twoNamedTables()
	db.Fields = append(db.Fields,
		schemamodel.Field{StructName: "A", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true},
		schemamodel.Field{StructName: "B", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true},
	)
	return db
}

func rlsHostStructTwoTablesFixture() schemamodel.Database {
	db := twoNamedTables()
	db.Roles = []schemamodel.Role{{Name: "app_reader", Inherit: true}}
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
	db.Roles = []schemamodel.Role{{Name: "app_reader", Inherit: true}}
	db.RLSEnabledTables = []schemamodel.RLSEnabledTable{{StructName: "T", Dialects: []string{"sqlserver"}}}
	db.RLSPolicies = []schemamodel.RLSPolicy{{
		StructName: "T", Name: "t_read", PolicyFor: "SELECT",
		ToRoles: "app_reader", UsingExpression: "true", Dialects: []string{"sqlserver"},
	}}
	return db
}

func rlsHostTableOnlyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{Name: "app_reader", Inherit: true}}
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
			Facets: must.Must(schemaext.NewFacets(&pgtable.DesiredPartition{
				Type:  "HASH",
				Parts: []pgtable.PartitionPart{{Expr: "(id % 4)"}},
			})),
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

// columnEnumForeignKeyFixture is the second reader of [schemamodel.Field.Enum]
// the ablation sees.
//
// The first is the column above: a dialect without standalone enums builds it
// from the values it carries, so ablating them leaves the bare type ENUM. The
// second is a refusal: MySQL refuses a foreign key between two ENUM columns
// whose value lists differ. Ablating the field makes the two lists equal, the
// refusal stops, and the render answers SQL where it answered an error -- which
// [everyCell] sees, because it keeps a refusal as its own text.
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
			Facets: must.Must(schemaext.NewFacets(&pgtable.DesiredPartition{
				Type:  "RANGE",
				Parts: []pgtable.PartitionPart{{Name: "tenant"}},
			})),
		}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "Tenant", Name: "tenant", Type: "VARCHAR(16)"},
		},
	}
}

// tablePartitionMemberFixture holds a partition that is partitioned itself, since
// an offline conversion describes partition keys only for tables that hold one.
func tablePartitionMemberFixture() schemamodel.Database {
	db := tablePartitionFixture()
	db.Tables = append(db.Tables, schemamodel.Table{
		StructName: "A", Name: "a",
		Facets: must.Must(schemaext.NewFacets(
			&pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{
				ParentSchema: "app", Parent: "t", Bound: "FOR VALUES FROM ('a') TO ('m')",
			}},
			&pgtable.DesiredPartition{Type: "LIST", Parts: []pgtable.PartitionPart{{Name: "tenant"}}},
		)),
	})
	return db
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
	return oneTable("T", schemamodel.Table{Name: "t", Facets: must.Must(pgtable.WithPersistence(schemaext.Facets{}, pgtable.Persistence{Unlogged: true}))})
}

func tableVirtualFixture() schemamodel.Database {
	return schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "T", Name: "t",
			Facets: must.Must(schemaext.NewFacets(&sqlitetable.DesiredVirtual{
				Virtual: sqlitetable.Virtual{Module: "fts5", Arguments: "body, tokenize='porter'"},
			})),
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
	db.Constraints[0].Facets = must.Must(pgtable.WithDeleteColumns(db.Constraints[0].Facets, []string{"parent_id"}))
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

func constraintExcludeFixture(initially string) schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true})
	db.FeatureObjects = must.Must(schemaext.NewObjects(declaredExtension("btree_gist"), schemaext.Object{
		Ref: pgtable.ExclusionRef("t", "t_excl", platformidentifier.ForDialect("postgres")),
		Value: &pgtable.DesiredExclusion{StructName: "T", Exclusion: pgtable.Exclusion{
			Elements: "s WITH =", UsingMethod: "gist", WhereCondition: "s IS NOT NULL",
			Deferrable: true, Initially: initially, Comment: "Distinct non-null values",
			Dependencies: []objectidentity.ID{pgextension.Subject(pgextension.Ref("btree_gist"))},
		}},
	}))
	db.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Desired))
	db.FeatureCoverage = must.Must(db.FeatureCoverage.Combine(must.Must(pgtable.ExclusionCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))))
	return db
}

func constraintPrimaryKeyMethodFixture() schemamodel.Database {
	db := constraintPrimaryKeyFixture()
	db.Constraints[0].UsingMethod = "HASH"
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

// indexHashFixture declares a HASH index. MariaDB records the method on every
// storage engine, so its comparison reads the method back; see
// mysqlindex.MethodSatisfiedBy.
func indexHashFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"}, Type: "HASH",
	}}
	return db
}

func indexFullTextFixture() schemamodel.Database {
	db := indexedTable()
	db.FeatureObjects = must.Must(schemaext.NewObjects(declaredExtension("pg_trgm")))
	db.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Desired))
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Type: "GIN", Operator: "gin_trgm_ops",
		Requires: []objectidentity.ID{pgextension.Subject(pgextension.Ref("pg_trgm"))},
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

func sequenceFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Sequences = []schemamodel.Sequence{{
		StructName: "S", Name: "order_seq", Schema: "public", AsType: "bigint",
		Start: new(int64(5)), Increment: new(int64(2)), MinValue: new(int64(1)), MaxValue: new(int64(999)), Cache: new(int64(4)),
		Cycle: true, IfNotExists: true, OwnedBy: "t.id", Comment: "order numbers",
	}}
	return db
}

func viewFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Views = []schemamodel.View{{
		StructName: "V", Name: "recent", Comment: "recent rows",
		Body: "SELECT id FROM t", WithCheck: true,
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb", "sqlserver"},
	}}
	return db
}

// viewAttributesFixture declares a view with SQL Server's own WITH clause. It
// is apart from viewFixture because every other target refuses the clause, and
// a refused pair would take the view's other fields out of the measurement
// there too.
func viewAttributesFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Views = []schemamodel.View{{
		StructName: "V", Name: "bound", Body: "SELECT id FROM t", Attributes: []string{"SCHEMABINDING"},
	}}
	return db
}

// viewLocalCheckFixture declares a view whose check option is LOCAL. It is
// apart from viewFixture because the targets whose check option has no level
// refuse the view, and a refused pair would take the view's other fields out
// of the measurement there too.
func viewLocalCheckFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Views = []schemamodel.View{{
		StructName: "V", Name: "own_rows", Body: "SELECT id FROM t", WithCheck: true, LocalCheck: true,
	}}
	return db
}

// viewOptionsFixture declares a view with PostgreSQL's options. It is apart
// from viewFixture because the targets without them refuse the view, and a
// refused pair would take the view's other fields out of the measurement there
// too.
func viewOptionsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Views = []schemamodel.View{{
		StructName: "V", Name: "guarded", Body: "SELECT id FROM t",
		Options: []string{"security_invoker=true", "security_barrier=true"},
	}}
	return db
}

// matViewOptionsFixture declares a materialized view with a storage
// parameter, apart from matViewFixture for the reason viewOptionsFixture
// gives.
func matViewOptionsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.MaterializedViews = []schemamodel.MaterializedView{{
		StructName: "MV", Name: "packed", Body: "SELECT id FROM t", Options: []string{"fillfactor=70"},
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
		Facets: must.Must(pgroutine.WithProperties(schemaext.Facets{}, pgroutine.Properties{Leakproof: true, Parallel: "SAFE", Strict: true})), Kind: "function",
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
	db.FeatureObjects = must.Must(schemaext.NewObjects(timescaleExtension()))
	db.FeatureCoverage = must.Must(tsschema.CompleteCoverage(schemaext.Desired))
	return db
}

// timescaleExtension declares the extension a hypertable and a continuous
// aggregate need to its owner, so a plan installs it ahead of them.
func timescaleExtension() schemaext.Object {
	return declaredExtension("timescaledb")
}

// declaredExtension declares the named extension to its owner with no
// setting of its own, as an index's or a constraint's operator class needs
// it.
func declaredExtension(name string) schemaext.Object {
	return must.Must(pgextension.DesiredObject(pgextension.Ref(name), pgextension.DesiredExtension{}))
}

func continuousAggregateFixture() schemamodel.Database {
	hypertable := &tsschema.DesiredHypertable{Column: "at"}
	db := oneTable("T", schemamodel.Table{Name: "t", Facets: must.Must(schemaext.NewFacets(hypertable))},
		schemamodel.Field{StructName: "T", FieldName: "At", Name: "at", Type: "TIMESTAMP", Nullable: true})
	db.FeatureObjects = must.Must(schemaext.NewObjects(timescaleExtension(), tsschema.DesiredContinuousAggregateObject("public", "t_hourly",
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
		Comment: "reader", Dialects: []string{"postgres", "yugabytedb"},
	}}
	return db
}

// cockroachRoleFixture declares every role attribute CockroachDB has an
// option for. CockroachDB has no SUPERUSER and no NOINHERIT, and refuses a
// role that declares either, so the role fixture above leaves it out.
func cockroachRoleFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{
		StructName: "RO", Name: "app_reader", Login: true, Password: "s3cret",
		CreateDB: true, CreateRole: true, Inherit: true, Replication: true,
		Comment: "reader", Dialects: []string{"cockroachdb"},
	}}
	return db
}

// roleGroupFixture declares a group, a principal of its own kind. No built-in
// target holds one, so each refuses it by capability; the YDB provider renders
// it as CREATE GROUP.
func roleGroupFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "readers", Group: true, Inherit: true}}
	return db
}

// roleMembershipFixture declares a role that is a member of another. No
// built-in target plans a membership, so each refuses it by capability; the
// YDB provider renders it as ALTER GROUP ... ADD USER.
func roleMembershipFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{
		{StructName: "RO", Name: "readers", Inherit: true},
		{StructName: "RO", Name: "app", Login: true, Inherit: true, MemberOf: []string{"readers"}},
	}
	return db
}

// grantDatabaseFixture grants a permission on the database itself. No built-in
// target plans one, so each refuses it by capability; the YDB provider renders
// it on the database root.
func grantDatabaseFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app", Login: true, Inherit: true}}
	db.Grants = []schemamodel.Grant{{StructName: "G", Role: "app", Privileges: []string{"CONNECT"}, OnDatabase: true}}
	return db
}

// grantObjectFixture grants a permission on a topic, a scheme object whose
// kind no other field of a grant names. No built-in target holds a topic, so
// each refuses the grant by capability; the YDB provider renders it.
func grantObjectFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app", Login: true, Inherit: true}}
	db.Grants = []schemamodel.Grant{{
		StructName: "G", Role: "app", Privileges: []string{"SELECT ROW"}, OnObject: "app.events",
		ObjectKind: schemamodel.GrantObjectTopic,
	}}
	return db
}

func grantTableFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true, Inherit: true}}
	db.Grants = []schemamodel.Grant{{
		StructName: "G", Role: "app_reader", Privileges: []string{"SELECT"}, OnTable: "t",
		WithOption: true, GrantedBy: "postgres", Comment: "read",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func grantSchemaFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true, Inherit: true}}
	db.Grants = []schemamodel.Grant{{
		StructName: "G", Role: "app_reader", Privileges: []string{"USAGE"}, OnSchema: "public",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func grantSequenceFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true, Inherit: true}}
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
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true, Inherit: true}}
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
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true, Inherit: true}}
	db.Grants = []schemamodel.Grant{{
		StructName: "G", Role: "app_reader", Privileges: []string{"UPDATE"}, OnTable: "t", Columns: []string{"label"},
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

// revokedGrantFixture asserts a privilege absent.
func revokedGrantFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true, Inherit: true}}
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
		{StructName: "OWNER", Name: "app_owner", Login: true, Inherit: true},
		{StructName: "READER", Name: "app_reader", Login: true, Inherit: true},
	}
	return db
}

// defaultPrivilegeTablesFixture declares TABLES privileges and a source comment.
func defaultPrivilegeTablesFixture() schemamodel.Database {
	return defaultPrivilegeFixture("app", "TABLES", "app_reader", pgdefaultprivilege.Desired{
		Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}},
		Comment:    "new tables in app are readable",
	})
}

func defaultPrivilegeSequencesFixture() schemamodel.Database {
	return defaultPrivilegeFixture("app", "SEQUENCES", "app_reader", pgdefaultprivilege.Desired{
		Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE"}},
	})
}

func defaultPrivilegeFunctionsFixture() schemamodel.Database {
	return defaultPrivilegeFixture("app", "FUNCTIONS", "app_reader", pgdefaultprivilege.Desired{
		Privileges: []pgdefaultprivilege.Grant{{Privilege: "EXECUTE"}},
	})
}

func defaultPrivilegeTypesFixture() schemamodel.Database {
	return defaultPrivilegeFixture("app", "TYPES", "app_reader", pgdefaultprivilege.Desired{
		Privileges: []pgdefaultprivilege.Grant{{Privilege: "USAGE"}},
	})
}

// Mixing plain and grantable rights makes grantability observable per privilege.
func defaultPrivilegeGrantableFixture() schemamodel.Database {
	return defaultPrivilegeFixture("app", "TABLES", "app_reader", pgdefaultprivilege.Desired{
		Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}, {Privilege: "INSERT", WithOption: true}},
	})
}

// A global revoke emits a statement even on a fresh database: PUBLIC inherits
// EXECUTE until the default ACL takes it away.
func defaultPrivilegeGlobalFixture() schemamodel.Database {
	return defaultPrivilegeFixture("", "FUNCTIONS", "PUBLIC", pgdefaultprivilege.Desired{Revoked: []string{"EXECUTE"}})
}

func defaultPrivilegeFixture(schema, class, grantee string, value pgdefaultprivilege.Desired) schemamodel.Database {
	db := defaultPrivilegeBase()
	ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{
		Grantor: "app_owner", Schema: schema, ObjectType: class, Grantee: grantee,
	}, platformidentifier.ForDialect(platform.Postgres)))
	object := must.Must(pgdefaultprivilege.DesiredObject(ref, value))
	object.Targets = []string{platform.Postgres, platform.CockroachDB, platform.YugabyteDB}
	db.FeatureObjects = must.Must(schemaext.NewObjects(object))
	db.FeatureCoverage = must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Desired))
	return db
}

func rlsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Inherit: true}}
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
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Inherit: true}}
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
	db.Tables[0].PrimaryKeyComment = "lookup"
	db.Tables[0].PrimaryKeyFacets = must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8}))
	return db
}

func primaryKeyConstraintOptionsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Constraints = []schemamodel.Constraint{{StructName: "T", Table: "t", Name: "PRIMARY", Type: "PRIMARY KEY", Columns: []string{"id"}, Comment: "lookup",
		Facets: must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8}))}}
	return db
}

func partitionModifiersFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t", Facets: must.Must(schemaext.NewFacets(&pgtable.DesiredPartition{Type: "RANGE", Parts: []pgtable.PartitionPart{{Name: "label", Collation: `"C"`, OperatorClass: "text_pattern_ops"}}}))},
		schemamodel.Field{StructName: "T", FieldName: "Label", Name: "label", Type: "TEXT"})
}

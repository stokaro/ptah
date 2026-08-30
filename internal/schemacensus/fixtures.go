package schemacensus

import (
	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/schemamodel"
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
	return []Fixture{
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
		{Name: "table-pk-parts", Schema: tablePrimaryKeyPartsFixture()},
		{Name: "table-partition", Schema: tablePartitionFixture()},
		{Name: "table-mysql", Schema: tableMySQLFixture()},
		{Name: "table-sqlite", Schema: tableSQLiteFixture()},
		{Name: "table-virtual", Schema: tableVirtualFixture()},
		{Name: "table-api", Schema: tableAPIFixture()},
		{Name: "table-override", Schema: tableOverrideFixture()},
		{Name: "table-rowttl", Schema: tableRowTTLFixture()},
		{Name: "table-row-deletion", Schema: tableRowDeletionFixture()},
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
		{Name: "index-include", Schema: indexIncludeFixture()},
		{Name: "index-parts", Schema: indexPartsFixture()},
		{Name: "index-storage", Schema: indexStorageFixture()},
		{Name: "index-concurrent", Schema: indexConcurrentFixture()},
		{Name: "index-clickhouse", Schema: indexClickHouseFixture()},
		{Name: "index-fulltext", Schema: indexFullTextFixture()},
		{Name: "enum", Schema: enumFixture()},
		{Name: "domain", Schema: domainFixture()},
		{Name: "composite", Schema: compositeFixture()},
		{Name: "range", Schema: rangeFixture()},
		{Name: "sequence", Schema: sequenceFixture()},
		{Name: "extension", Schema: extensionFixture()},
		{Name: "view", Schema: viewFixture()},
		{Name: "matview", Schema: matViewFixture()},
		{Name: "matview-refresh", Schema: matViewRefreshFixture()},
		{Name: "function", Schema: functionFixture()},
		{Name: "trigger", Schema: triggerFixture()},
		{Name: "hypertable", Schema: hypertableFixture()},
		{Name: "continuous-aggregate", Schema: continuousAggregateFixture()},
		{Name: "synonym", Schema: synonymFixture()},
		{Name: "extended-property", Schema: extendedPropertyFixture()},
		{Name: "role", Schema: roleFixture()},
		{Name: "grant-table", Schema: grantTableFixture()},
		{Name: "grant-schema", Schema: grantSchemaFixture()},
		{Name: "grant-sequence", Schema: grantSequenceFixture()},
		{Name: "rls", Schema: rlsFixture()},
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
		{Name: "constraint-deferrable-only", Schema: constraintDeferrableOnlyFixture()},
		{Name: "constraint-initially-only", Schema: constraintInitiallyOnlyFixture()},
		{Name: "constraint-host-table-only", Schema: constraintHostTableOnlyFixture()},
		{Name: "index-host-table-only", Schema: indexHostTableOnlyFixture()},
		{Name: "domain-default-only", Schema: domainDefaultOnlyFixture()},
		{Name: "domain-default-expr-only", Schema: domainDefaultExprOnlyFixture()},
		{Name: "trigger-body-only", Schema: triggerBodyOnlyFixture()},
		{Name: "trigger-foreach-statement", Schema: triggerForEachStatementFixture()},
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
	}
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
	db.Roles = []schemamodel.Role{{Name: "app_reader", Login: true}}
	db.RLSEnabledTables = []schemamodel.RLSEnabledTable{{StructName: "B"}}
	db.RLSPolicies = []schemamodel.RLSPolicy{{
		StructName: "B", Name: "b_read", PolicyFor: "SELECT",
		ToRoles: "app_reader", UsingExpression: "true",
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
	db.Roles = []schemamodel.Role{{Name: "app_reader", Login: true}}
	db.RLSEnabledTables = []schemamodel.RLSEnabledTable{{StructName: "T"}}
	db.RLSPolicies = []schemamodel.RLSPolicy{{
		StructName: "T", Name: "t_read", PolicyFor: "SELECT",
		ToRoles: "app_reader", UsingExpression: "true",
	}}
	return db
}

func rlsHostTableOnlyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{Name: "app_reader", Login: true}}
	db.RLSEnabledTables = []schemamodel.RLSEnabledTable{{Table: "t"}}
	db.RLSPolicies = []schemamodel.RLSPolicy{{
		Table: "t", Name: "t_read", PolicyFor: "SELECT",
		ToRoles: "app_reader", UsingExpression: "true",
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

func columnMySQLFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{
			StructName: "T", FieldName: "S", Name: "s", Type: "VARCHAR(32)", Nullable: true,
			Charset: "utf8mb4", Collate: "utf8mb4_bin",
		},
		schemamodel.Field{
			StructName: "T", FieldName: "U", Name: "u", Type: "TIMESTAMP", Nullable: true,
			UpdateExpression: "CURRENT_TIMESTAMP",
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
		Name: "t", Engine: "InnoDB", AutoIncrement: "100",
		Charset: "utf8mb4", Collate: "utf8mb4_bin",
	})
}

func tableSQLiteFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{Name: "t", Strict: true, WithoutRowID: true})
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

func tableRowTTLFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{
		Name: "t",
		RowTTL: &ast.RowTTLSpec{
			ExpireAfter: "3 months", ExpirationExpression: "created_at", JobCron: "@daily",
			SelectBatchSize: new(int64(500)), DeleteBatchSize: new(int64(100)), DeleteRateLimit: new(int64(100)),
			SelectRateLimit: new(int64(100)), RowStatsPollInterval: "1m", Pause: new(true),
			LabelMetrics: new(true), DisableChangefeedReplication: new(true),
		},
	}, schemamodel.Field{StructName: "T", FieldName: "CreatedAt", Name: "created_at", Type: "TIMESTAMP", Nullable: true})
	return db
}

func tableRowDeletionFixture() schemamodel.Database {
	return oneTable("T", schemamodel.Table{
		Name:              "t",
		RowDeletionPolicy: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "30d"},
	}, schemamodel.Field{StructName: "T", FieldName: "CreatedAt", Name: "created_at", Type: "TIMESTAMP", Nullable: true})
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
		Condition: "s IS NOT NULL", NullsDistinct: new(false),
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

func indexClickHouseFixture() schemamodel.Database {
	db := indexedTable()
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Type: "minmax", Granularity: 4,
	}}
	return db
}

func indexFullTextFixture() schemamodel.Database {
	db := indexedTable()
	db.Extensions = []schemamodel.Extension{{Name: "pg_trgm"}}
	db.Indexes = []schemamodel.Index{{
		StructName: "T", Name: "idx_t_s", TableName: "t", Fields: []string{"s"},
		Type: "GIN", Operator: "gin_trgm_ops", Parser: "ngram",
		RequiresExtensions: []string{"pg_trgm"},
	}}
	return db
}

func enumFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Enums = []schemamodel.Enum{{Name: "mood", Schema: "public", Values: []string{"ok", "bad"}}}
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

func matViewRefreshFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.MaterializedViews = []schemamodel.MaterializedView{{
		StructName: "MV", Name: "daily", Body: "SELECT id FROM t",
		Refresh: &ast.MatViewRefreshSpec{
			Mode: "scheduled", Interval: "1 hour", Offset: "5 minutes",
			Randomize: "1 minute", Append: true, DependsOn: []string{"other"},
		},
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

func triggerFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Functions = []schemamodel.Function{{
		StructName: "F", Name: "touch", Returns: "trigger", Language: "plpgsql",
		Body: "BEGIN RETURN NEW; END;", Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	db.Triggers = []schemamodel.Trigger{{
		StructName: "TR", Name: "t_touch", Table: "t", Timing: "BEFORE", Event: "UPDATE",
		ForEach: "ROW", ExecuteFunction: "touch()", Body: "BEGIN RETURN NEW; END;",
		Comment: "touch", Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	return db
}

func hypertableFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "At", Name: "at", Type: "TIMESTAMP", Nullable: true})
	db.Extensions = []schemamodel.Extension{{Name: "timescaledb"}}
	db.Hypertables = []schemamodel.Hypertable{{
		StructName: "HY", Table: "t", Column: "at", ChunkInterval: "1 day",
		IfNotExists: true, Comment: "time series",
	}}
	return db
}

func continuousAggregateFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"},
		schemamodel.Field{StructName: "T", FieldName: "At", Name: "at", Type: "TIMESTAMP", Nullable: true})
	db.Extensions = []schemamodel.Extension{{Name: "timescaledb"}}
	db.Hypertables = []schemamodel.Hypertable{{StructName: "HY", Table: "t", Column: "at"}}
	db.ContinuousAggregates = []schemamodel.ContinuousAggregate{{
		StructName: "CA", Name: "t_hourly", Schema: "public",
		Body: "SELECT id FROM t", MaterializedOnly: new(true), Comment: "hourly",
	}}
	return db
}

func synonymFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Synonyms = []schemamodel.Synonym{{
		StructName: "SY", Name: "tt", Schema: "dbo", Target: "dbo.t", Comment: "alias",
	}}
	return db
}

func extendedPropertyFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.ExtendedProperties = []schemamodel.ExtendedProperty{{
		StructName: "XP", Name: "MS_Description", Schema: "dbo", Table: "t", Column: "id",
		Value: "identifier", Comment: "docs",
	}}
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

func rlsFixture() schemamodel.Database {
	db := oneTable("T", schemamodel.Table{Name: "t"})
	db.Roles = []schemamodel.Role{{StructName: "RO", Name: "app_reader", Login: true}}
	db.RLSEnabledTables = []schemamodel.RLSEnabledTable{{
		StructName: "T", Table: "t", Comment: "rls",
		Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
	}}
	db.RLSPolicies = []schemamodel.RLSPolicy{{
		StructName: "T", Name: "t_read", Table: "t", PolicyFor: "SELECT",
		ToRoles: "app_reader", UsingExpression: "true", WithCheckExpression: "true",
		Comment: "read policy", Dialects: []string{"postgres", "cockroachdb", "yugabytedb"},
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

package ydb_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer/internal/dialects/ydb"
	"ptah.run/internal/ydbgap"
)

// keyed builds a one-column table whose single column is col, keyed on id.
func keyed(column *ast.ColumnNode) *ast.CreateTableNode {
	return &ast.CreateTableNode{
		Name:    "t",
		Columns: []*ast.ColumnNode{ast.NewColumn("id", "BIGINT").SetPrimary(), column},
	}
}

// withIndex builds a keyed table carrying one inline index.
func withIndex(index *ast.IndexNode, columns ...*ast.ColumnNode) *ast.CreateTableNode {
	table := &ast.CreateTableNode{Name: "t", Columns: append([]*ast.ColumnNode{ast.NewColumn("id", "BIGINT").SetPrimary()}, columns...)}
	table.AddIndex(index)
	return table
}

// alter wraps one operation in an ALTER TABLE of t.
func alter(operation ast.AlterOperation) *ast.AlterTableNode {
	return &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{operation}}
}

// TestRender_RefusesByCapability_FailurePath pins every refusal a capability
// key decides. Each names the key, so a YDB release that gains the ability is a
// preset change; each was measured as the server's own refusal of the
// statement the renderer would otherwise write.
func TestRender_RefusesByCapability_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		node    ast.Node
		wantKey capability.Capability
		wantErr string
	}{
		{name: "a table without a key", caps: capability.YDB262(),
			node:    &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{ast.NewColumn("n", "BIGINT")}},
			wantKey: capability.PrimaryKeyRequired,
			wantErr: `table "t" declares no primary key, which requires target capability primary_key_required, unavailable on this ydb target`},
		{name: "a CHECK on a column", caps: capability.YDB262(),
			node: keyed(&ast.ColumnNode{Name: "n", Type: "INTEGER", Nullable: true, Check: "n > 0"}), wantKey: capability.CheckConstraints,
			wantErr: `column "n" of table "t" declares CHECK \(n > 0\), which requires target capability check_constraints, .*`},
		{name: "a CHECK constraint", caps: capability.YDB262(),
			node: &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{ast.NewColumn("id", "BIGINT").SetPrimary()},
				Constraints: []*ast.ConstraintNode{{Type: ast.CheckConstraint, Name: "ck", Expression: "id > 0"}}},
			wantKey: capability.CheckConstraints, wantErr: `constraint "ck" on table "t" is a CHECK, which requires target capability check_constraints, .*`},
		{name: "a foreign key on a column", caps: capability.YDB262(),
			node:    keyed(&ast.ColumnNode{Name: "p", Type: "BIGINT", Nullable: true, ForeignKey: &ast.ForeignKeyRef{Table: "p", Column: "id"}}),
			wantKey: capability.ForeignKeys, wantErr: `column "p" of table "t" references p, which requires target capability foreign_keys, .*`},
		{name: "a generated column", caps: capability.YDB262(),
			node: keyed(&ast.ColumnNode{Name: "g", Type: "BIGINT", Nullable: true, GeneratedExpression: "id * 2"}), wantKey: capability.GeneratedColumns,
			wantErr: `column "g" of table "t" is generated, which requires target capability generated_columns, .*`},
		{name: "a named NOT NULL", caps: capability.YDB262(),
			node: keyed(&ast.ColumnNode{Name: "n", Type: "BIGINT", NotNullConstraintName: "nn"}), wantKey: capability.NamedNotNullConstraints,
			wantErr: `column "n" of table "t" names its NOT NULL constraint nn, .*`},
		{name: "an enum column", caps: capability.YDB262(),
			node: keyed(&ast.ColumnNode{Name: "s", Type: "mood", Nullable: true, EnumType: true}), wantKey: capability.EnumInlineColumn,
			wantErr: `column "s" of table "t" is of an enum type mood, which requires target capability enum_inline_column, .*`},
		{name: "an expression default", caps: capability.YDB262(),
			node: keyed(ast.NewColumn("ts", "TIMESTAMP").SetDefaultExpression("CURRENT_TIMESTAMP")), wantKey: capability.ExpressionDefaults,
			wantErr: `column "ts" of table "t" defaults to the expression CURRENT_TIMESTAMP, which requires target capability expression_defaults, .*`},
		{name: "a Decimal of a chosen precision on 25.1", caps: capability.YDB251(),
			node: keyed(ast.NewColumn("price", "DECIMAL(10,2)")), wantKey: capability.ParameterizedDecimal,
			wantErr: `column "price" of table "t": DECIMAL\(10,2\) \(this line has Decimal\(22,9\) only\), which requires target capability parameterized_decimal, .*`},
		{name: "a wide date type on 25.1", caps: capability.YDB251(),
			node: keyed(ast.NewColumn("d", "Date32")), wantKey: capability.WideDateTimeTypes,
			wantErr: `column "d" of table "t": Date32 .*, which requires target capability wide_date_time_types, .*`},
		{name: "a 16-bit default on 25.1", caps: capability.YDB251(),
			node: keyed(ast.NewColumn("n", "SMALLINT").SetDefault("1")), wantKey: capability.SmallIntegerDefaults,
			wantErr: `column "n" of table "t": a default of type Int16 .*, which requires target capability small_integer_defaults, .*`},
		{name: "a document default on 25.2", caps: capability.YDB252(),
			node: keyed(ast.NewColumn("doc", "JSONB").SetDefault("{}")), wantKey: capability.DocumentTypeDefaults,
			wantErr: `column "doc" of table "t": a default of type JsonDocument .*, which requires target capability document_type_defaults, .*`},
		{name: "a serial where the line has none", caps: capability.YDB262().With(capability.SerialColumns, false),
			node: keyed(ast.NewColumn("s", "SERIAL")), wantKey: capability.SerialColumns,
			wantErr: `column "s" of table "t": SERIAL .*, which requires target capability serial_columns, .*`},
		{name: "a Serial's start where the line has no sequence settings",
			caps: capability.YDB262().With(capability.SerialSequenceOptions, false),
			node: &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{
				{Name: "id", Type: "BIGSERIAL", Primary: true, AutoInc: true, IdentityStart: "100"}}},
			wantKey: capability.SerialSequenceOptions,
			wantErr: `column "id" of table "t" gives its sequence a start or an increment, which requires target capability serial_sequence_options, .*`},
		{name: "a Serial's sequence changed where the line has no sequence settings",
			caps:    capability.YDB262().With(capability.SerialSequenceOptions, false),
			node:    &ast.AlterSerialSequenceNode{Table: "t", Column: "id", Path: "/local/t/_serial_column_id", Start: 1, Increment: 5},
			wantKey: capability.SerialSequenceOptions,
			wantErr: `changing the sequence of Serial column "id" of table "t", which requires target capability serial_sequence_options, .*`},
		{name: "an asynchronous index where the line has none", caps: capability.YDB262().With(capability.AsyncIndexes, false),
			node: withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"}, Type: "async"}, ast.NewColumn("a", "TEXT")), wantKey: capability.AsyncIndexes,
			wantErr: `index "i" is asynchronous, which requires target capability async_indexes, .*`},
		{name: "an invisible index", caps: capability.YDB262(),
			node: withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"}, Invisible: true}, ast.NewColumn("a", "TEXT")), wantKey: capability.InvisibleIndexes,
			wantErr: `index "i" is invisible, which requires target capability invisible_indexes, .*`},
		{name: "a unique index added to a table that exists", caps: capability.YDB262(),
			node: &ast.IndexNode{Name: "u", Table: "t", Columns: []string{"a"}, Unique: true}, wantKey: capability.UniqueIndexOnExistingTable,
			wantErr: `unique index "u" is added to table "t", which exists already .*, which requires target capability unique_index_on_existing_table, .*`},
		{name: "a guarded index drop", caps: capability.YDB262(),
			node: &ast.DropIndexNode{Name: "i", Table: "t", IfExists: true}, wantKey: capability.DropIndexIfExists,
			wantErr: `DROP INDEX i IF EXISTS, which requires target capability drop_index_if_exists, .*`},
		{name: "a column added with a default before 26.1", caps: capability.YDB253(),
			node: alter(&ast.AddColumnOperation{Column: ast.NewColumn("n", "BIGINT").SetDefault("1")}), wantKey: capability.AddColumnWithDefault,
			wantErr: `adding column "n" to table "t" with a default, which requires target capability add_column_with_default, .*`},
		{name: "a key column added", caps: capability.YDB262(),
			node: alter(&ast.AddColumnOperation{Column: ast.NewColumn("k", "BIGINT").SetPrimary()}), wantKey: capability.PrimaryKeyAlterable,
			wantErr: `adding column "k" to table "t" as part of the key, which requires target capability primary_key_alterable, .*`},
		{name: "a column made NOT NULL", caps: capability.YDB262(),
			node:    alter(&ast.ModifyColumnOperation{Column: ast.NewColumn("n", "BIGINT").SetNotNull(), Changed: ast.ColumnProperties{Nullability: true}, HasChanged: true}),
			wantKey: capability.AlterColumnSetNotNull, wantErr: `making column "n" of table "t" NOT NULL, which requires target capability alter_column_set_not_null, .*`},
		{name: "a column made nullable where the line cannot", caps: capability.YDB262().With(capability.AlterColumnDropNotNull, false),
			node:    alter(&ast.AlterColumnOperation{ColumnName: "n", Action: ast.AlterColumnDropNotNull}),
			wantKey: capability.AlterColumnDropNotNull, wantErr: `making column "n" of table "t" nullable, which requires target capability alter_column_drop_not_null, .*`},
		{name: "a default changed on 26.1", caps: capability.YDB261(),
			node:    alter(&ast.ModifyColumnOperation{Column: ast.NewColumn("n", "BIGINT").SetDefault("2"), Changed: ast.ColumnProperties{Default: true}, HasChanged: true}),
			wantKey: capability.AlterColumnDefault, wantErr: `changing the default of column "n" of table "t", which requires target capability alter_column_default, .*`},
		{name: "a type changed", caps: capability.YDB262(),
			node:    alter(&ast.ModifyColumnOperation{Column: ast.NewColumn("n", "BIGINT"), Changed: ast.ColumnProperties{Type: true}, HasChanged: true}),
			wantKey: capability.AlterColumnType, wantErr: `changing the type of column "n" of table "t" to BIGINT, which requires target capability alter_column_type, .*`},
		{name: "a column renamed", caps: capability.YDB262(),
			node: alter(&ast.RenameColumnOperation{OldName: "a", NewName: "b"}), wantKey: capability.RenameColumnClause,
			wantErr: `renaming column "a" of table "t", which requires target capability rename_column_clause, .*`},
		{name: "a key added", caps: capability.YDB262(),
			node:    alter(&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: []string{"a"}}}),
			wantKey: capability.PrimaryKeyAlterable, wantErr: `adding a primary key to table "t", which requires target capability primary_key_alterable, .*`},
		{name: "a constraint dropped", caps: capability.YDB262(),
			node: alter(&ast.DropConstraintOperation{ConstraintName: "c"}), wantKey: capability.DropConstraintGeneric,
			wantErr: `dropping constraint "c" of table "t", which requires target capability drop_constraint_generic, .*`},
		{name: "an ALGORITHM clause", caps: capability.YDB262(),
			node:    &ast.AlterTableNode{Name: "t", Algorithm: "INSTANT", Operations: []ast.AlterOperation{&ast.DropColumnOperation{ColumnName: "a"}}},
			wantKey: capability.AlterTableAlgorithmLock, wantErr: `ALTER TABLE t asks for ALGORITHM=INSTANT LOCK=, .*`},
		{name: "an enum type", caps: capability.YDB262(),
			node: &ast.EnumNode{Name: "mood", Values: []string{"a"}}, wantKey: capability.EnumCustomType,
			wantErr: `enum mood, which requires target capability enum_custom_type, .*`},
		{name: "a sequence", caps: capability.YDB262(),
			node: &ast.CreateSequenceNode{Name: "s"}, wantKey: capability.Sequences, wantErr: `sequence s, which requires target capability sequences, .*`},
		{name: "a function", caps: capability.YDB262(),
			node: &ast.CreateFunctionNode{Name: "f"}, wantKey: capability.Functions, wantErr: `function f, which requires target capability functions, .*`},
		{name: "a trigger", caps: capability.YDB262(),
			node: &ast.CreateTriggerNode{Name: "tr"}, wantKey: capability.Triggers, wantErr: `trigger tr, which requires target capability triggers, .*`},
		{name: "a materialized view", caps: capability.YDB262(),
			node: &ast.CreateMaterializedViewNode{Name: "mv"}, wantKey: capability.MaterializedViews,
			wantErr: `materialized view mv, which requires target capability materialized_views, .*`},
		{name: "row-level security", caps: capability.YDB262(),
			node: &ast.CreatePolicyNode{Name: "p", Table: "t"}, wantKey: capability.RowLevelSecurity,
			wantErr: `policy p, which requires target capability row_level_security, .*`},
		{name: "a guard where the target has none", caps: capability.YDB262().With(capability.ObjectExistenceGuards, false),
			node: &ast.DropTableNode{Name: "t", IfExists: true}, wantKey: capability.ObjectExistenceGuards,
			wantErr: `DROP TABLE IF EXISTS t, which requires target capability object_existence_guards, .*`},
		{name: "a create guard where the target has none", caps: capability.YDB262().With(capability.ObjectExistenceGuards, false),
			node:    &ast.CreateTableNode{Name: "t", IfNotExists: true, Columns: []*ast.ColumnNode{ast.NewColumn("id", "BIGINT").SetPrimary()}},
			wantKey: capability.ObjectExistenceGuards,
			wantErr: `CREATE TABLE IF NOT EXISTS t, which requires target capability object_existence_guards, .*`},
		{name: "a covering index where the target has none", caps: capability.YDB262().With(capability.IndexCoveringColumns, false),
			node:    withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"}, IncludeColumns: []string{"b"}}, ast.NewColumn("a", "TEXT"), ast.NewColumn("b", "TEXT")),
			wantKey: capability.IndexCoveringColumns,
			wantErr: `index "i" covers columns, which requires target capability index_covering_columns, .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Feature, qt.Equals, string(test.wantKey))
			c.Assert(capabilityErr.Dialect, qt.Equals, "ydb")
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestRender_RefusesWhatYDBCannotHold_FailurePath pins the refusals no key
// decides: a shape YDB refuses on every measured line, quoted where the server
// said it, and an object family a later phase of the plan implements.
func TestRender_RefusesWhatYDBCannotHold_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{name: "a key over a type YDB cannot order",
			node:    &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{ast.NewColumn("id", "DOUBLE PRECISION").SetPrimary()}},
			wantErr: `the primary key of table "t": column "id" is Double, which YDB refuses in a key \(` + "`wrong key type Double`" + `\)`},
		{name: "an index over the key itself",
			node:    withIndex(&ast.IndexNode{Name: "i", Columns: []string{"id"}}),
			wantErr: `index "i" on table "t": its columns are the table's key, .*index keys shouldn't be table keys.*`},
		{name: "an index covering a key column",
			node:    withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"}, IncludeColumns: []string{"id"}}, ast.NewColumn("a", "TEXT")),
			wantErr: `index "i" on table "t": it covers key column "id", which every YDB index carries already`},
		{name: "an index over a type YDB cannot order",
			node:    withIndex(&ast.IndexNode{Name: "i", Columns: []string{"j"}}, ast.NewColumn("j", "JSON")),
			wantErr: `index "i" on table "t": column "j" is Json, which YDB refuses as an index key`},
		{name: "a unique asynchronous index",
			node:    withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"}, Unique: true, Type: "async"}, ast.NewColumn("a", "TEXT")),
			wantErr: `index "i": a unique index is synchronous on YDB .*`},
		{name: "a partial index",
			node:    withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"}, Condition: "a IS NOT NULL"}, ast.NewColumn("a", "TEXT")),
			wantErr: `index "i": YDB has no partial index`},
		{name: "a descending index column",
			node:    withIndex(&ast.IndexNode{Name: "i", Parts: []ast.IndexPart{{Name: "a", Desc: true}}}, ast.NewColumn("a", "TEXT")),
			wantErr: `index "i": a YDB index column has no order`},
		{name: "an expression index",
			node:    withIndex(&ast.IndexNode{Name: "i", Parts: []ast.IndexPart{{Expr: "lower(a)"}}}, ast.NewColumn("a", "TEXT")),
			wantErr: `index "i": YDB has no expression index`},
		{name: "an access method YDB has no index for",
			node:    withIndex(&ast.IndexNode{Name: "i", Columns: []string{"a"}, Type: "gin"}, ast.NewColumn("a", "TEXT")),
			wantErr: `index "i": index method "gin" has no YDB counterpart: .*`},
		{name: "a standalone index with a guard",
			node:    &ast.IndexNode{Name: "i", Table: "t", Columns: []string{"a"}, IfNotExists: true},
			wantErr: `index "i": YDB's ADD INDEX has no IF NOT EXISTS guard`},
		{name: "a serial added to a table that exists",
			node:    alter(&ast.AddColumnOperation{Column: ast.NewColumn("s", "SERIAL")}),
			wantErr: `adding column "s" to table "t": YDB adds no Serial column to an existing table .*`},
		{name: "a NOT NULL column added without a default",
			node:    alter(&ast.AddColumnOperation{Column: ast.NewColumn("n", "BIGINT").SetNotNull()}),
			wantErr: `adding column "n" to table "t": YDB adds a NOT NULL column only with a default .*`},
		{name: "a modification that does not say what changed",
			node:    alter(&ast.ModifyColumnOperation{Column: ast.NewColumn("n", "BIGINT")}),
			wantErr: `column "n" of table "t": YDB has no MODIFY COLUMN; .*`},
		{name: "a NULL default on a NOT NULL column",
			node:    keyed(&ast.ColumnNode{Name: "n", Type: "TEXT", Default: &ast.DefaultValue{Value: "NULL", ValueSet: true}}),
			wantErr: `column "n" of table "t": the column is NOT NULL and its default is NULL`},
		{name: "a default the type cannot hold",
			node:    keyed(ast.NewColumn("n", "TINYINT").SetDefault("300")),
			wantErr: `column "n" of table "t": default "300" has no YDB counterpart: it is not a Int8 value`},
		{name: "a type YDB has none of",
			node:    keyed(ast.NewColumn("at", "TIME")),
			wantErr: `column "at" of table "t": TIME has no YDB counterpart: YDB has no time-of-day type`},
		{name: "an identity generated always",
			node: &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{
				{Name: "id", Type: "BIGINT", Primary: true, AutoInc: true, IdentityGeneration: "ALWAYS"}}},
			wantErr: `column "id" of table "t": GENERATED ALWAYS refuses an explicit value, and a YDB Serial takes one`},
		{name: "raw identity options",
			node: &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{
				{Name: "id", Type: "BIGINT", Primary: true, AutoInc: true, IdentityOptions: "CACHE 10"}}},
			wantErr: `column "id" of table "t": a YDB Serial's sequence takes a start and an increment and nothing else, .* CACHE 10`},
		{name: "a start below 1",
			node: &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{
				{Name: "id", Type: "BIGINT", Primary: true, AutoInc: true, IdentityStart: "0"}}},
			wantErr: `column "id" of table "t": identity_start 0: a YDB Serial's sequence takes a start of 1 or more .*`},
		{name: "an increment below 1",
			node: &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{
				{Name: "id", Type: "BIGINT", Primary: true, AutoInc: true, IdentityIncrement: "-2"}}},
			wantErr: `column "id" of table "t": identity_increment -2: a YDB Serial's sequence takes an increment of 1 or more .*`},
		{name: "a start that is not a whole number",
			node: &ast.CreateTableNode{Name: "t", Columns: []*ast.ColumnNode{
				{Name: "id", Type: "BIGSERIAL", Primary: true, IdentityStart: "1.5", IdentityGeneration: "BY_DEFAULT"}}},
			wantErr: `column "id" of table "t": identity_start "1.5" is not a whole number YDB's ALTER SEQUENCE takes`},
		{name: "a collation",
			node:    keyed(&ast.ColumnNode{Name: "n", Type: "TEXT", Nullable: true, Collate: "C"}),
			wantErr: `column "n" of table "t": YDB has no column collation .*`},
		{name: "a character set that is not UTF-8",
			node:    keyed(&ast.ColumnNode{Name: "n", Type: "TEXT", Nullable: true, Charset: "latin1"}),
			wantErr: `column "n" of table "t": YDB stores text as UTF-8 only, .*`},
		{name: "a table comment",
			node:    &ast.CreateTableNode{Name: "t", Comment: "x", Columns: []*ast.ColumnNode{ast.NewColumn("id", "BIGINT").SetPrimary()}},
			wantErr: `the comment on table "t": ` + regexp.QuoteMeta(ydbgap.Comments.Message())},
		{name: "a column comment",
			node:    keyed(&ast.ColumnNode{Name: "n", Type: "TEXT", Nullable: true, Comment: "x"}),
			wantErr: `the comment on column "n" of table "t": storing a comment on a YDB object is not implemented yet .*`},
		{name: "two YDB table settings name the first in order",
			node:    &ast.CreateTableNode{Name: "t", Options: map[string]string{"TTL": "x", "AUTO_PARTITIONING_BY_SIZE": "ENABLED"}, Columns: []*ast.ColumnNode{ast.NewColumn("id", "BIGINT").SetPrimary()}},
			wantErr: `the table option AUTO_PARTITIONING_BY_SIZE=ENABLED on table "t": setting YDB table options .*`},
		{name: "a YDB table setting",
			node:    &ast.CreateTableNode{Name: "t", Options: map[string]string{"TTL": "x"}, Columns: []*ast.ColumnNode{ast.NewColumn("id", "BIGINT").SetPrimary()}},
			wantErr: `the table option TTL=x on table "t": setting YDB table options .* is not implemented yet .*`},
		{name: "a row deletion policy",
			node:    &ast.CreateTableNode{Name: "t", RowDeletionPolicy: &ast.RowDeletionPolicySpec{Column: "ts", Interval: "1 day"}, Columns: []*ast.ColumnNode{ast.NewColumn("id", "BIGINT").SetPrimary()}},
			wantErr: `the row deletion policy on table "t": setting YDB table options .* is not implemented yet .*`},
		{name: "a PostgreSQL partition",
			node:    &ast.CreateTableNode{Name: "t", Partition: &ast.PartitionSpec{Type: "RANGE"}, Columns: []*ast.ColumnNode{ast.NewColumn("id", "BIGINT").SetPrimary()}},
			wantErr: `table "t": PARTITION BY is PostgreSQL's; .*`},
		{name: "a view", node: &ast.CreateViewNode{Name: "v"},
			wantErr: `view v: managing YDB views is not implemented yet .*`},
		{name: "a role", node: &ast.CreateRoleNode{Name: "r"},
			wantErr: `role r: managing YDB users, groups and permissions is not implemented yet .*`},
		{name: "an upsert", node: ast.NewUpsert("t"),
			wantErr: `upsert into t: YDB's UPSERT INTO matches on the primary key .*; build the statement with core/query's UpsertInto`},
		{name: "a database", node: &ast.CreateDatabaseNode{Name: "d"},
			wantErr: `CREATE DATABASE d: YDB has no CREATE DATABASE statement; a YDB database is created by the cluster's administrators`},
		{name: "a dropped table with CASCADE", node: &ast.DropTableNode{Name: "t", Cascade: true},
			wantErr: `DROP TABLE t CASCADE: YDB's DROP TABLE has no CASCADE; .*`},
		{name: "an extension", node: &ast.ExtensionNode{Name: "pg_trgm"},
			wantErr: `extension pg_trgm: YDB has no extensions`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(capability.YDB262()).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(got, qt.Equals, "")
		})
	}
}

// TestRender_AlterIsAllOrNothing_FailurePath pins that a refused operation
// leaves no prefix of an ALTER behind: the statements before it are checked,
// not written, until every operation has passed. It drives VisitNode and reads
// the buffer, because Render clears the buffer on any error and would hide a
// prefix written before the refusal.
func TestRender_AlterIsAllOrNothing_FailurePath(t *testing.T) {
	c := qt.New(t)
	node := &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{
		&ast.DropColumnOperation{ColumnName: "a"},
		&ast.RenameColumnOperation{OldName: "b", NewName: "c"},
	}}
	r := ydb.NewWithCapabilities(capability.YDB262())

	err := r.VisitNode(node)

	c.Assert(err, qt.ErrorMatches, `renaming column "b" of table "t", .*`)
	c.Assert(r.Output(), qt.Equals, "")
}

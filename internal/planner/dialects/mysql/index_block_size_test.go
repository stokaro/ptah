package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
	"ptah.run/engine/builtin"
	"ptah.run/internal/mysqlindex"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/migration/schemadiff"
)

// blockSizeIndex is one index's side of a block-size comparison.
type blockSizeIndex struct {
	columns   []string
	size      uint64
	invisible bool
}

// planBlockSize plans the index of desired against current on a table of
// rowFormat, as the owner compares and plans it, and renders the plan.
func planBlockSize(c *qt.C, dialect, rowFormat string, desired, current blockSizeIndex) string {
	c.Helper()
	runtime := must.Must(builtin.New())
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "INT", Primary: true},
			{StructName: "T", Name: "a", Type: "INT", Nullable: true},
			{StructName: "T", Name: "b", Type: "INT", Nullable: true},
		},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "idx_a", Fields: desired.columns, TableName: "t", Invisible: desired.invisible,
			Facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, desired.size))}},
		FeatureCoverage: must.Must(mysqlsource.BlockSizeCoverage()),
	}
	observed := &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Schema: "app", RowFormat: rowFormat, Columns: []catalog.Column{
			{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "a", DataType: "int", ColumnType: "int", IsNullable: "YES", OrdinalPosition: 2},
			{Name: "b", DataType: "int", ColumnType: "int", IsNullable: "YES", OrdinalPosition: 3},
		}}},
		Indexes: []catalog.Index{{Name: "idx_a", TableName: "t", Schema: "app", Columns: current.columns, Invisible: current.invisible,
			Facets: must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{},
				mysqlschema.ObservedIndexBlockSize{KeyBlockSize: current.size, Retained: mysqlindex.KeepsBlockSize(dialect, rowFormat)}))}},
		Constraints: []catalog.Constraint{{Name: "PRIMARY", TableName: "t", Schema: "app", Type: "PRIMARY KEY", ColumnName: "id"}},
	}
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = "app"
	caps := capability.ForDialect(dialect)
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), declared, observed,
		catalog.ServerInfo{Dialect: dialect, Schema: "app", IdentifierSemantics: semantics, Capabilities: caps}, nil, runtime)
	c.Assert(err, qt.IsNil)
	nodes, err := mysql.NewForDialect(dialect, caps).GenerateMigrationAST(c.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(dialect, caps, nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// TestPlanner_IndexBlockSize writes the statements a changed block size
// needs. The owner replaces an index whose hint alone changes, with the table
// copy MySQL needs to store it, and plans nothing where MySQL discards the
// hint. Where the common plan already replaces the index for another reason,
// that statement writes the declared hint and the owner adds nothing: MySQL
// 8.4.11 stores the new hint when the in-place rebuild also changes the key.
// A visibility change runs in place and keeps the old hint, so the owner
// still replaces the index beside it.
func TestPlanner_IndexBlockSize(t *testing.T) {
	a, ab := []string{"a"}, []string{"a", "b"}
	for _, test := range []struct {
		name, dialect, rowFormat string
		desired, current         blockSizeIndex
		want                     string
	}{
		{
			name: "MySQL, the hint alone", dialect: "mysql", rowFormat: "Compressed",
			desired: blockSizeIndex{columns: a, size: 8}, current: blockSizeIndex{columns: a, size: 4},
			want: "-- Modify table: t\n-- ALTER statements: --\nALTER TABLE `t` DROP INDEX `idx_a`, ADD INDEX `idx_a` (`a`) KEY_BLOCK_SIZE=8, ALGORITHM=COPY;\n\n",
		},
		{
			name: "MySQL, the hint removed", dialect: "mysql", rowFormat: "Compressed",
			desired: blockSizeIndex{columns: a}, current: blockSizeIndex{columns: a, size: 4},
			want: "-- Modify table: t\n-- ALTER statements: --\nALTER TABLE `t` DROP INDEX `idx_a`, ADD INDEX `idx_a` (`a`), ALGORITHM=COPY;\n\n",
		},
		{
			name: "MySQL, a row format that discards the hint", dialect: "mysql", rowFormat: "Dynamic",
			desired: blockSizeIndex{columns: a, size: 8}, current: blockSizeIndex{columns: a},
			want: "",
		},
		{
			name: "MySQL, the key changes too", dialect: "mysql", rowFormat: "Compressed",
			desired: blockSizeIndex{columns: ab, size: 8}, current: blockSizeIndex{columns: a, size: 4},
			want: "-- Modify table: t\n-- ALTER statements: --\nALTER TABLE `t` DROP INDEX `idx_a`, ADD INDEX `idx_a` (`a`, `b`) KEY_BLOCK_SIZE=8;\n\n",
		},
		{
			name: "MySQL, the visibility changes too", dialect: "mysql", rowFormat: "Compressed",
			desired: blockSizeIndex{columns: a, size: 8, invisible: true}, current: blockSizeIndex{columns: a, size: 4},
			want: "-- Modify table: t\n-- ALTER statements: --\nALTER TABLE `t` DROP INDEX `idx_a`, ADD INDEX `idx_a` (`a`) KEY_BLOCK_SIZE=8 INVISIBLE, ALGORITHM=COPY;\n\n" +
				"-- ALTER statements: --\nALTER TABLE `app`.`t` ALTER INDEX `idx_a` INVISIBLE;\n\n",
		},
		{
			name: "MariaDB, the hint alone", dialect: "mariadb", rowFormat: "Dynamic",
			desired: blockSizeIndex{columns: a, size: 8}, current: blockSizeIndex{columns: a, size: 4},
			want: "-- Modify table: t\n-- ALTER statements: --\nALTER TABLE `t` DROP INDEX `idx_a`, ADD INDEX `idx_a` (`a`) KEY_BLOCK_SIZE=8;\n\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(planBlockSize(c, test.dialect, test.rowFormat, test.desired, test.current), qt.Equals, test.want)
		})
	}
}

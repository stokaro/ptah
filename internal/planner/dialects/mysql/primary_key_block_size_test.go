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

// primaryKeyState is one side of a primary key comparison.
type primaryKeyState struct {
	columns []string
	size    uint64
	comment string
}

// planPrimaryKey plans table t's primary key from current to desired on a
// table of rowFormat, as the common plan and the MySQL owner plan it, and
// renders the plan. The declaration writes the key on the table, as a Go
// table directive and a SQL file do.
func planPrimaryKey(c *qt.C, dialect, rowFormat string, desired, current primaryKeyState) string {
	c.Helper()
	runtime := must.Must(builtin.New())
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: desired.columns, PrimaryKeyComment: desired.comment,
			PrimaryKeyFacets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, desired.size))}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "INT"},
			{StructName: "T", Name: "a", Type: "INT"},
		},
		FeatureCoverage: must.Must(mysqlsource.BlockSizeCoverage()),
	}
	observed := &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Schema: "app", RowFormat: rowFormat, Columns: []catalog.Column{
			{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "a", DataType: "int", ColumnType: "int", IsNullable: "NO", OrdinalPosition: 2},
		}}},
		Constraints: []catalog.Constraint{{Name: "PRIMARY", TableName: "t", Schema: "app", Type: "PRIMARY KEY", ColumnNames: current.columns,
			Comment: current.comment, Facets: must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{},
				mysqlschema.ObservedIndexBlockSize{KeyBlockSize: current.size, Retained: mysqlindex.KeepsBlockSize(dialect, rowFormat)}))}},
		FeatureCoverage: must.Must(mysqlschema.IndexBlockSizeCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)),
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

// TestPlanner_PrimaryKeyBlockSize writes the statements a primary key's
// changed block size needs. The owner replaces a key whose hint alone
// changes, asking MySQL for the table copy that stores it: an in-place
// replacement that changes nothing else keeps the old hint. Where MySQL
// discards the hint, nothing is planned. Where the common plan replaces the
// key for another reason, that statement writes the declared hint in place
// and the owner adds nothing, as it does on MariaDB, which stores the hint in
// place in every case.
func TestPlanner_PrimaryKeyBlockSize(t *testing.T) {
	id, both := []string{"id"}, []string{"id", "a"}
	for _, test := range []struct {
		name, dialect, rowFormat string
		desired, current         primaryKeyState
		want                     string
	}{
		{"MySQL, the hint alone", "mysql", "Compressed", primaryKeyState{columns: id, size: 8}, primaryKeyState{columns: id, size: 4},
			"-- Modify table: t\n-- ALTER statements: --\nALTER TABLE `t` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8, ALGORITHM=COPY;\n\n"},
		{"MySQL, the hint removed", "mysql", "Compressed", primaryKeyState{columns: id}, primaryKeyState{columns: id, size: 4},
			"-- Modify table: t\n-- ALTER statements: --\nALTER TABLE `t` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`), ALGORITHM=COPY;\n\n"},
		{"MySQL, a row format that discards the hint", "mysql", "Dynamic", primaryKeyState{columns: id, size: 8}, primaryKeyState{columns: id},
			""},
		{"MySQL, the comment changes too", "mysql", "Compressed", primaryKeyState{columns: id, size: 8, comment: "new"},
			primaryKeyState{columns: id, size: 4, comment: "old"},
			"-- Modify table: t\n-- ALTER statements: --\nALTER TABLE `t` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8 COMMENT 'new';\n\n"},
		{"MySQL, the columns change too", "mysql", "Compressed", primaryKeyState{columns: both, size: 8}, primaryKeyState{columns: id, size: 4},
			"-- Modify table: t\n-- ALTER statements: --\nALTER TABLE `t` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`, `a`) KEY_BLOCK_SIZE=8;\n\n"},
		{"MariaDB, the hint alone", "mariadb", "Dynamic", primaryKeyState{columns: id, size: 8}, primaryKeyState{columns: id, size: 4},
			"-- Modify table: t\n-- ALTER statements: --\nALTER TABLE `t` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8;\n\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(planPrimaryKey(c, test.dialect, test.rowFormat, test.desired, test.current), qt.Equals, test.want)
		})
	}
}

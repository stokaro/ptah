package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/migration/schemadiff"
)

// primaryKeyState is one side of a primary key comparison.
type primaryKeyState struct {
	size    uint64
	comment string
}

// planPrimaryKey plans table t's primary key from current to desired on a
// compressed table, which keeps a block-size hint on both engines.
func planPrimaryKey(c *qt.C, dialect string, desired, current primaryKeyState) string {
	c.Helper()
	runtime := must.Must(builtin.New())
	declared := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"},
			PrimaryKeyBlockSize: desired.size, PrimaryKeyComment: desired.comment}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "INT", Primary: true},
			{StructName: "T", Name: "a", Type: "INT", Nullable: true},
		},
	}
	observed := &catalog.Database{
		Tables: []catalog.Table{{Name: "t", Schema: "app", RowFormat: "Compressed", Columns: []catalog.Column{
			{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "a", DataType: "int", ColumnType: "int", IsNullable: "YES", OrdinalPosition: 2},
		}}},
		Constraints: []catalog.Constraint{{Name: "PRIMARY", TableName: "t", Schema: "app", Type: "PRIMARY KEY", ColumnName: "id",
			KeyBlockSize: current.size, Comment: current.comment}},
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

// TestPlanner_PrimaryKeyBlockSize replaces a primary key for its block size.
// MySQL keeps the old hint through an in-place replacement that changes
// nothing else, so the replacement asks for the table copy that stores it;
// one that also changes the comment stores it in place, and so does MariaDB.
func TestPlanner_PrimaryKeyBlockSize(t *testing.T) {
	for _, test := range []struct {
		name, dialect    string
		desired, current primaryKeyState
		want             string
	}{
		{"MySQL, the hint alone", "mysql", primaryKeyState{size: 8}, primaryKeyState{size: 4},
			"ALTER TABLE `t` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8, ALGORITHM=COPY;"},
		{"MySQL, the hint removed", "mysql", primaryKeyState{}, primaryKeyState{size: 4},
			"ALTER TABLE `t` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`), ALGORITHM=COPY;"},
		{"MySQL, the comment changes too", "mysql", primaryKeyState{size: 8, comment: "new"}, primaryKeyState{size: 4, comment: "old"},
			"ALTER TABLE `t` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8 COMMENT 'new';"},
		{"MariaDB, the hint alone", "mariadb", primaryKeyState{size: 8}, primaryKeyState{size: 4},
			"ALTER TABLE `t` DROP PRIMARY KEY, ADD PRIMARY KEY (`id`) KEY_BLOCK_SIZE=8;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(planPrimaryKey(c, test.dialect, test.desired, test.current), qt.Contains, test.want)
		})
	}
}

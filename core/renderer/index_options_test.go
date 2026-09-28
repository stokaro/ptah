package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// indexOptionsSchema is table t with a commented, invisible index over a.
func indexOptionsSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "int", Primary: true},
			{StructName: "T", Name: "a", Type: "int", Nullable: true},
		},
		Indexes: []schemamodel.Index{{
			StructName: "T", Name: "k_a", TableName: "t", Fields: []string{"a"}, Comment: "it's a lookup", Invisible: true,
		}},
	}
}

// TestRender_IndexOptions_HappyPath writes an index's comment and visibility
// in each MySQL-family dialect's own words; each engine answers ERROR 1064 to
// the other's (stokaro/ptah#3853).
func TestRender_IndexOptions_HappyPath(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
		want    string
	}{
		{
			dialect: platform.MySQL, caps: capability.MySQL84(),
			want: "CREATE INDEX `k_a` ON `t` (`a`) COMMENT 'it''s a lookup' INVISIBLE;",
		},
		{
			dialect: platform.MariaDB, caps: capability.MariaDB1011(),
			want: "CREATE INDEX `k_a` ON `t` (`a`) COMMENT 'it''s a lookup' IGNORED;",
		},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(indexOptionsSchema(), test.dialect, test.caps)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestRender_IndexOptions_FailurePath refuses an invisible index on a target
// without one, rather than build it visible and change the query plans its
// author held back.
func TestRender_IndexOptions_FailurePath(t *testing.T) {
	tests := []struct {
		dialect string
		caps    capability.Capabilities
	}{
		{dialect: platform.Postgres, caps: capability.Postgres18()},
		{dialect: platform.MySQL, caps: capability.MySQLLegacy()},
		{dialect: platform.MariaDB, caps: capability.MariaDBLegacy()},
		{dialect: platform.SQLite, caps: capability.SQLite3()},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(indexOptionsSchema(), test.dialect, test.caps)

			c.Assert(err, qt.ErrorMatches, `.*index "k_a" is invisible, which requires target capability invisible_indexes, `+
				`unavailable on this `+test.dialect+` target`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestRender_IndexStatements_HappyPath writes the rebuild of an index under its
// own name as one statement, and a visibility change in place. MySQL 8.4.11
// takes `DROP INDEX k, ADD INDEX k (...)` on an index a foreign key needs, and
// refuses the DROP INDEX alone with ERROR 1553.
func TestRender_IndexStatements_HappyPath(t *testing.T) {
	rebuilt := ast.NewIndex("k_q", "c", "q")
	rebuilt.Comment = "fk new"
	tests := []struct {
		name      string
		dialect   string
		caps      capability.Capabilities
		operation ast.AlterOperation
		want      string
	}{
		{
			name: "a rebuild", dialect: platform.MySQL, caps: capability.MySQL84(),
			operation: &ast.ReplaceIndexOperation{Index: rebuilt},
			want:      "ALTER TABLE `c` DROP INDEX `k_q`, ADD INDEX `k_q` (`q`) COMMENT 'fk new';",
		},
		{
			name: "MySQL hides an index", dialect: platform.MySQL, caps: capability.MySQL84(),
			operation: &ast.AlterIndexVisibilityOperation{IndexName: "k_q", Invisible: true},
			want:      "ALTER TABLE `c` ALTER INDEX `k_q` INVISIBLE;",
		},
		{
			name: "MariaDB shows an index", dialect: platform.MariaDB, caps: capability.MariaDB1011(),
			operation: &ast.AlterIndexVisibilityOperation{IndexName: "k_q", Invisible: false},
			want:      "ALTER TABLE `c` ALTER INDEX `k_q` NOT IGNORED;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQLWithCapabilities(test.dialect, test.caps,
				&ast.AlterTableNode{Name: "c", Operations: []ast.AlterOperation{test.operation}})

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRender_IndexStatements_FailurePath refuses a visibility change on a
// target without invisible indexes.
func TestRender_IndexStatements_FailurePath(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQLWithCapabilities(platform.Postgres, capability.Postgres18(),
		&ast.AlterTableNode{Name: "c", Operations: []ast.AlterOperation{
			&ast.AlterIndexVisibilityOperation{IndexName: "k_q", Invisible: true},
		}})

	c.Assert(err, qt.ErrorMatches, `.*index "k_q" is invisible, which requires target capability invisible_indexes, .*`)
	c.Assert(sql, qt.Equals, "")
}

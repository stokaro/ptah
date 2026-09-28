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
// in each dialect's own words; MySQL and MariaDB answer ERROR 1064 to each
// other's (stokaro/ptah#3853), and CockroachDB prints its index back with NOT
// VISIBLE.
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
		{
			dialect: platform.CockroachDB, caps: capability.CockroachDB263(),
			want: `ON "t" ("a") NOT VISIBLE;`,
		},
		{
			dialect: platform.CockroachDB, caps: capability.CockroachDB25(),
			want: `ON "t" ("a") NOT VISIBLE;`,
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

// TestRender_CockroachDBInvisiblePartialIndex_HappyPath puts the visibility
// after the condition: CockroachDB v26.3.2 refuses `NOT VISIBLE WHERE ...` with
// a syntax error.
func TestRender_CockroachDBInvisiblePartialIndex_HappyPath(t *testing.T) {
	c := qt.New(t)
	schema := indexOptionsSchema()
	schema.Indexes[0].Comment = ""
	schema.Indexes[0].Condition = "a > 0"

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, platform.CockroachDB,
		capability.CockroachDB263())

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `ON "t" ("a") WHERE a > 0 NOT VISIBLE;`)
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
		{
			name: "CockroachDB hides an index", dialect: platform.CockroachDB, caps: capability.CockroachDB263(),
			operation: &ast.AlterIndexVisibilityOperation{IndexName: "k_q", Invisible: true},
			want:      `ALTER INDEX "c"@"k_q" NOT VISIBLE;`,
		},
		{
			name: "CockroachDB shows an index", dialect: platform.CockroachDB, caps: capability.CockroachDB263(),
			operation: &ast.AlterIndexVisibilityOperation{IndexName: "k_q", Invisible: false},
			want:      `ALTER INDEX "c"@"k_q" VISIBLE;`,
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

// TestRender_PostgreSQLIndexVisibility_FailurePath refuses a
// visibility change on a PostgreSQL target whose capabilities were widened to
// claim invisible indexes: the statement the renderer knows is CockroachDB's,
// and PostgreSQL has no ALTER INDEX form for it.
func TestRender_PostgreSQLIndexVisibility_FailurePath(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQLWithCapabilities(platform.Postgres,
		capability.Postgres18().With(capability.InvisibleIndexes, true),
		&ast.AlterTableNode{Name: "c", Operations: []ast.AlterOperation{
			&ast.AlterIndexVisibilityOperation{IndexName: "k_q", Invisible: true},
		}})

	c.Assert(err, qt.ErrorMatches, `.*postgres: index "k_q" cannot be shown or hidden from the optimizer on this engine`)
	c.Assert(sql, qt.Equals, "")
}

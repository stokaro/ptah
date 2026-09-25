package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// deleteListSchema is a child whose composite key clears only parent_id when
// the parent row goes. tenant is NOT NULL, which is valid because the list
// leaves it alone.
func deleteListSchema(onDelete string, listed ...string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parents", PrimaryKey: []string{"tenant", "id"}},
			{StructName: "Child", Name: "children"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "tenant", Type: "INTEGER"},
			{StructName: "Parent", Name: "id", Type: "INTEGER"},
			{StructName: "Child", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: "tenant", Type: "INTEGER"},
			{StructName: "Child", Name: "parent_id", Type: "INTEGER", Nullable: true, Default: "0"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Child", Table: "children", Name: "fk_children_parent", Type: "FOREIGN KEY",
			Columns: []string{"tenant", "parent_id"}, ForeignTable: "parents",
			ForeignColumns: []string{"tenant", "id"},
			OnDelete:       onDelete, OnDeleteColumns: listed,
		}},
	}
}

func renderDeleteList(schema *schemamodel.Database, dialect string, caps capability.Capabilities) (string, error) {
	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(schema, dialect, caps)
	return strings.Join(statements, "\n"), err
}

// The list is written where the target takes it, and a NOT NULL column the
// list leaves alone does not refuse the key.
func TestRenderDeleteColumnList_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		caps     capability.Capabilities
		onDelete string
		want     string
	}{
		{name: "PostgreSQL 16", dialect: "postgres", caps: capability.Postgres16(), onDelete: "SET NULL", want: `ON DELETE SET NULL ("parent_id")`},
		{name: "PostgreSQL 18", dialect: "postgres", caps: capability.Postgres18(), onDelete: "SET NULL", want: `ON DELETE SET NULL ("parent_id")`},
		{name: "SET DEFAULT", dialect: "postgres", caps: capability.Postgres16(), onDelete: "SET DEFAULT", want: `ON DELETE SET DEFAULT ("parent_id")`},
		{name: "YugabyteDB 2025", dialect: "yugabytedb", caps: capability.YugabyteDB25(), onDelete: "SET NULL", want: `ON DELETE SET NULL ("parent_id")`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderDeleteList(deleteListSchema(tt.onDelete, "parent_id"), tt.dialect, tt.caps)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, tt.want)
		})
	}
}

// A target without the clause refuses the list. Rendering the action without
// it would clear tenant too, where the declaration clears parent_id alone.
func TestRenderDeleteColumnList_UnsupportedTarget(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		wantErr string
	}{
		{name: "PostgreSQL 14", dialect: "postgres", caps: capability.Postgres14(), wantErr: `postgres does not support a column list on ON DELETE SET NULL`},
		{name: "PostgreSQL 13", dialect: "postgres", caps: capability.Postgres13(), wantErr: `postgres does not support a column list on ON DELETE SET NULL`},
		{name: "YugabyteDB 2024", dialect: "yugabytedb", caps: capability.YugabyteDB24(), wantErr: `yugabytedb does not support a column list on ON DELETE SET NULL`},
		{name: "CockroachDB", dialect: "cockroachdb", caps: capability.ForDialect("cockroachdb"), wantErr: `cockroachdb does not support a column list on ON DELETE SET NULL`},
		{name: "MySQL", dialect: "mysql", caps: capability.ForDialect("mysql"), wantErr: `mysql does not support a column list on ON DELETE SET NULL`},
		{name: "MariaDB", dialect: "mariadb", caps: capability.ForDialect("mariadb"), wantErr: `mariadb does not support a column list on ON DELETE SET NULL`},
		{name: "SQLite", dialect: "sqlite", caps: capability.ForDialect("sqlite"), wantErr: `sqlite does not support a column list on ON DELETE SET NULL`},
		{name: "SQL Server", dialect: "sqlserver", caps: capability.ForDialect("sqlserver"), wantErr: `sqlserver does not support a column list on ON DELETE SET NULL`},
		{name: "Oracle", dialect: "oracle", caps: capability.ForDialect("oracle"), wantErr: `oracle does not support a column list on ON DELETE SET NULL`},
		{name: "ClickHouse", dialect: "clickhouse", caps: capability.ForDialect("clickhouse"), wantErr: `clickhouse does not support a column list on ON DELETE SET NULL`},
		// Spanner refuses the action before it reaches the list.
		{name: "Spanner", dialect: "spanner", caps: capability.ForDialect("spanner"), wantErr: `spanner does not support ON DELETE SET NULL`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderDeleteList(deleteListSchema("SET NULL", "parent_id"), tt.dialect, tt.caps)

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// A list no engine would accept is refused on a target that has the clause.
func TestRenderDeleteColumnList_InvalidDeclaration(t *testing.T) {
	tests := []struct {
		name     string
		onDelete string
		listed   []string
		wantErr  string
	}{
		{
			name: "the listed column is NOT NULL", onDelete: "SET NULL", listed: []string{"tenant"},
			wantErr: `invalid foreign key: foreign key on "children"\."tenant" uses SET NULL but the local column is NOT NULL`,
		},
		{
			name: "no list sets the NOT NULL column too", onDelete: "SET NULL", listed: nil,
			wantErr: `invalid foreign key: foreign key on "children"\."tenant" uses SET NULL but the local column is NOT NULL`,
		},
		{
			name: "a list after CASCADE", onDelete: "CASCADE", listed: []string{"parent_id"},
			wantErr: `invalid foreign key: an ON DELETE column list needs SET NULL or SET DEFAULT, not "CASCADE"`,
		},
		{
			name: "a column outside the key", onDelete: "SET NULL", listed: []string{"id"},
			wantErr: `invalid foreign key: ON DELETE SET NULL names column "id", which is not one of the key's columns \[tenant parent_id\]`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderDeleteList(deleteListSchema(tt.onDelete, tt.listed...), "postgres", capability.Postgres16())

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// deleteListTableNode is the same key built as an AST node, the way a caller
// that renders without a schema model writes it.
func deleteListTableNode(listed ...string) *ast.CreateTableNode {
	table := ast.NewCreateTable("children")
	table.AddColumn(ast.NewColumn("tenant", "INTEGER").SetNotNull())
	table.AddColumn(ast.NewColumn("parent_id", "INTEGER"))
	table.AddConstraint(ast.NewForeignKeyConstraint("fk_children_parent", []string{"tenant", "parent_id"}, &ast.ForeignKeyRef{
		Table:           "parents",
		Columns:         []string{"tenant", "id"},
		OnDelete:        "SET NULL",
		OnDeleteColumns: listed,
	}))
	return table
}

// The AST path reads the list the same way: it is written, and the NOT NULL
// column it leaves alone is accepted.
func TestRenderSQLDeleteColumnList_HappyPath(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL("postgres", deleteListTableNode("parent_id"))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ON DELETE SET NULL ("parent_id")`)
}

func TestRenderSQLDeleteColumnList_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		listed  []string
		wantErr string
	}{
		{
			name: "no list clears the NOT NULL column", dialect: "postgres", listed: nil,
			wantErr: `invalid foreign key: foreign key on "children"\."tenant" uses SET NULL but the local column is NOT NULL`,
		},
		{
			name: "a target without the clause", dialect: "mysql", listed: []string{"parent_id"},
			wantErr: `mysql does not support a column list on ON DELETE SET NULL`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(tt.dialect, deleteListTableNode(tt.listed...))

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

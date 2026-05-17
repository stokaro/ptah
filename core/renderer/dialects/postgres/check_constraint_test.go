package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/renderer/dialects/postgres"
)

// TestPostgreSQLRenderer_ColumnCheckConstraint covers issue #112 — column-level
// `check=` (and optional `check_name=`) annotations need to render as either
// inline `CHECK (...)` (relying on PostgreSQL's auto-naming convention
// `<table>_<column>_check`) or, when an explicit name is provided, the named
// `CONSTRAINT <name> CHECK (...)` form so the constraint survives drift
// detection round-trips.
func TestPostgreSQLRenderer_ColumnCheckConstraint(t *testing.T) {
	tests := []struct {
		name     string
		table    *ast.CreateTableNode
		expected string
	}{
		{
			name: "unnamed CHECK in column scope",
			table: ast.NewCreateTable("files").
				AddColumn(ast.NewColumn("id", "TEXT").SetPrimary()).
				AddColumn(ast.NewColumn("category", "TEXT").
					SetNotNull().
					SetDefault("other").
					SetCheck("category IN ('photos','invoices','documents','other')")),
			expected: `-- POSTGRES TABLE: files --
CREATE TABLE files (
  id TEXT PRIMARY KEY NOT NULL,
  category TEXT NOT NULL DEFAULT 'other' CHECK (category IN ('photos','invoices','documents','other'))
);

`,
		},
		{
			name: "named CHECK in column scope",
			table: ast.NewCreateTable("files").
				AddColumn(ast.NewColumn("id", "TEXT").SetPrimary()).
				AddColumn(ast.NewColumn("type", "TEXT").
					SetNotNull().
					SetCheck("type IN ('image','document','video','audio','archive','other')").
					SetCheckName("files_type_valid")),
			expected: `-- POSTGRES TABLE: files --
CREATE TABLE files (
  id TEXT PRIMARY KEY NOT NULL,
  type TEXT NOT NULL CONSTRAINT files_type_valid CHECK (type IN ('image','document','video','audio','archive','other'))
);

`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := postgres.New()
			result, err := renderer.Render(tt.table)

			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.Equals, tt.expected)
		})
	}
}

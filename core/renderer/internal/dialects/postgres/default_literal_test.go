package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/postgres"
)

// A column default reaches the renderer in one of two spellings, and both are
// legitimate: a struct tag arrives bare (`default="active"` -> `active`) and a
// schema file arrives as the literal it was written as (`DEFAULT 'active'` ->
// `'active'`). Quoting the second spelling again silently changes what it
// stands for.
//
// Measured against PostgreSQL 18 before the fix, a varchar default rendered as
//
//	DEFAULT '''x'''
//
// applied without complaint and stored the three-character value `'x'`, while
// the same treatment of a boolean default was refused with
// `invalid input syntax for type boolean: "'t'"` (stokaro/ptah#1582).
func TestPostgreSQLRenderer_DefaultIsQuotedExactlyOnce(t *testing.T) {
	tests := []struct {
		name     string
		table    *ast.CreateTableNode
		expected string
	}{
		{
			name: "a literal from a schema file keeps its own quotes",
			table: ast.NewCreateTable("items").
				AddColumn(ast.NewColumn("tag", "VARCHAR(20)").
					SetNotNull().
					SetDefault("'x'")),
			expected: `-- POSTGRES TABLE: items --
CREATE TABLE items (
  tag VARCHAR(20) NOT NULL DEFAULT 'x'
);

`,
		},
		{
			name: "a bare value from a struct tag is quoted",
			table: ast.NewCreateTable("items").
				AddColumn(ast.NewColumn("tag", "VARCHAR(20)").
					SetNotNull().
					SetDefault("x")),
			expected: `-- POSTGRES TABLE: items --
CREATE TABLE items (
  tag VARCHAR(20) NOT NULL DEFAULT 'x'
);

`,
		},
		{
			// The spelling PostgreSQL refused outright, rather than accepting
			// with the wrong value.
			name: "a boolean literal stays a boolean",
			table: ast.NewCreateTable("items").
				AddColumn(ast.NewColumn("ok", "BOOL").
					SetNotNull().
					SetDefault("'t'")),
			expected: `-- POSTGRES TABLE: items --
CREATE TABLE items (
  ok BOOL NOT NULL DEFAULT 't'
);

`,
		},
		{
			// A cast is why the two ends of the value cannot be tested on their
			// own: this one does not end in a quote.
			name: "a literal carrying a cast survives whole",
			table: ast.NewCreateTable("items").
				AddColumn(ast.NewColumn("meta", "JSONB").
					SetNotNull().
					SetDefault("'{}'::jsonb")),
			expected: `-- POSTGRES TABLE: items --
CREATE TABLE items (
  meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

`,
		},
		{
			// A bare value that contains a quote still has to be escaped, or
			// the statement stops parsing where the value does.
			name: "a bare value containing a quote is escaped",
			table: ast.NewCreateTable("items").
				AddColumn(ast.NewColumn("note", "TEXT").
					SetNotNull().
					SetDefault("it's")),
			expected: `-- POSTGRES TABLE: items --
CREATE TABLE items (
  note TEXT NOT NULL DEFAULT 'it''s'
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
			c.Assert(legacyPostgresSQL(result), qt.Equals, tt.expected)
		})
	}
}

// The same decision is made on the ALTER path, which is reached when a default
// is added to a column that already exists.
func TestPostgreSQLRenderer_AlterColumnDefaultIsQuotedExactlyOnce(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "a literal keeps its own quotes", value: "'x'", want: "SET DEFAULT 'x';"},
		{name: "a bare value is quoted", value: "x", want: "SET DEFAULT 'x';"},
		{name: "a literal carrying a cast survives whole", value: "'{}'::jsonb", want: "SET DEFAULT '{}'::jsonb;"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			alter := &ast.AlterTableNode{
				Name: "items",
				Operations: []ast.AlterOperation{
					&ast.ModifyColumnOperation{
						Column: ast.NewColumn("tag", "VARCHAR(20)").
							SetNotNull().
							SetDefault(tt.value),
					},
				},
			}

			out := legacyPostgresSQL(renderPG(t, alter))

			c.Assert(out, qt.Contains, tt.want)
			c.Assert(out, qt.Not(qt.Contains), `'''`)
		})
	}
}

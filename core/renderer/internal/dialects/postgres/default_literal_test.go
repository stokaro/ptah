package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer/internal/dialects/postgres"
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

// A quoted literal is not merely untidy: it says the value is a string when the
// column's type is not one.
//
// PostgreSQL casts it on the way in and reads it back unquoted, so the round
// trip converged and nothing reported the mismatch. Spanner does not cast.
// Measured on the PGAdapter emulator v0.55.2, before the fix:
//
//	ERROR: Error parsing the default value of column `probe`.`d_int`:
//	Expected type INT64; found STRING
//
// and the same refusal for BOOL on `DEFAULT 'true'`, which made every table
// carrying a non-string default impossible to create (stokaro/ptah#2073).
func TestPostgreSQLRenderer_DefaultIsRenderedInTheFormItsTypeTakes(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		value      string
		want       string
	}{
		{name: "an integer default is bare", columnType: "BIGINT", value: "7", want: "DEFAULT 7"},
		{name: "a negative integer default is bare", columnType: "INTEGER", value: "-1", want: "DEFAULT -1"},
		{name: "a fractional default is bare", columnType: "NUMERIC(18,2)", value: "1.50", want: "DEFAULT 1.50"},
		{name: "an exponent default is bare", columnType: "DOUBLE PRECISION", value: "1e5", want: "DEFAULT 1e5"},
		{name: "a boolean default is bare", columnType: "BOOLEAN", value: "true", want: "DEFAULT true"},
		{name: "a boolean default is lowercased", columnType: "BOOL", value: "FALSE", want: "DEFAULT false"},
		{
			// The type decides, not the value. A text column whose default
			// happens to read as a number still stores the characters.
			name:       "a number in a text column keeps its quotes",
			columnType: "TEXT", value: "7", want: "DEFAULT '7'",
		},
		{
			// PostgreSQL takes NaN only as 'NaN'::numeric, so a renderer that
			// trusted strconv here would emit DDL the server refuses.
			name:       "a non-finite number keeps its quotes",
			columnType: "NUMERIC", value: "NaN", want: "DEFAULT 'NaN'",
		},
		{
			name:       "a value that only starts as a number keeps its quotes",
			columnType: "INTEGER", value: "7 or 8", want: "DEFAULT '7 or 8'",
		},
		{
			// An array default is not an element default: the bare spellings
			// are `{1,2}` and `ARRAY[1,2]`, and neither is a number.
			name:       "an array default keeps its quotes",
			columnType: "INTEGER[]", value: "{1,2}", want: "DEFAULT '{1,2}'",
		},
		{
			name:       "a quoted numeric literal is still passed through untouched",
			columnType: "BIGINT", value: "'7'", want: "DEFAULT '7'",
		},
		{
			name:       "an unclassified type keeps its quotes",
			columnType: "money", value: "1.00", want: "DEFAULT '1.00'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			table := ast.NewCreateTable("probe").
				AddColumn(ast.NewColumn("value", tt.columnType).SetDefault(tt.value))

			result, err := postgres.New().Render(table)

			c.Assert(err, qt.IsNil)
			c.Assert(legacyPostgresSQL(result), qt.Contains, tt.want)
		})
	}
}

// The ALTER path makes the same decision, and it is the one a live database
// reaches: a default added to a column that already exists.
func TestPostgreSQLRenderer_AlterColumnDefaultIsRenderedInTheFormItsTypeTakes(t *testing.T) {
	tests := []struct {
		name       string
		columnType string
		value      string
		want       string
	}{
		{name: "an integer default is bare", columnType: "BIGINT", value: "7", want: "SET DEFAULT 7;"},
		{name: "a boolean default is bare", columnType: "BOOLEAN", value: "true", want: "SET DEFAULT true;"},
		{name: "a string default keeps its quotes", columnType: "TEXT", value: "x", want: "SET DEFAULT 'x';"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			alter := &ast.AlterTableNode{
				Name: "items",
				Operations: []ast.AlterOperation{
					&ast.ModifyColumnOperation{
						Column: ast.NewColumn("value", tt.columnType).SetDefault(tt.value),
					},
				},
			}

			c.Assert(legacyPostgresSQL(renderPG(t, alter)), qt.Contains, tt.want)
		})
	}
}

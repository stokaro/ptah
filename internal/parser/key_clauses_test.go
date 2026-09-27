package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// uniqueOf is what a UNIQUE the table declares carries.
type uniqueOf struct {
	Name          string
	Columns       []string
	NullsDistinct *bool
}

// tableUniques lists the table-level UNIQUE constraints of the one CREATE
// TABLE in sql, and the columns that carry a UNIQUE of their own.
func tableUniques(c *qt.C, sql string) ([]uniqueOf, []string) {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.Postgres)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	table := statements.Statements[0].(*ast.CreateTableNode)
	var uniques []uniqueOf
	for _, constraint := range table.Constraints {
		if constraint.Type == ast.UniqueConstraint {
			uniques = append(uniques, uniqueOf{Name: constraint.Name, Columns: constraint.Columns, NullsDistinct: constraint.NullsDistinct})
		}
	}
	var unique []string
	for _, column := range table.Columns {
		if column.Unique {
			unique = append(unique, column.Name)
		}
	}
	return uniques, unique
}

// TestParse_ColumnUniqueNullsClause_HappyPath reads a column's UNIQUE with a
// NULLS [NOT] DISTINCT clause as the table's UNIQUE over the column, where the
// model keeps the clause. Measured on PostgreSQL 18.6, `a int UNIQUE NULLS NOT
// DISTINCT` and `UNIQUE NULLS NOT DISTINCT (a)` build the same constraint under
// the same name. Refused before, as `unsupported column attribute: NULLS`
// (stokaro/ptah#3821).
func TestParse_ColumnUniqueNullsClause_HappyPath(t *testing.T) {
	notDistinct, distinct := false, true
	tests := []struct {
		name        string
		sql         string
		wantUniques []uniqueOf
		wantColumns []string
	}{
		{
			name:        "NULLS NOT DISTINCT",
			sql:         "CREATE TABLE c (a int UNIQUE NULLS NOT DISTINCT, b int UNIQUE);",
			wantUniques: []uniqueOf{{Columns: []string{"a"}, NullsDistinct: &notDistinct}},
			wantColumns: []string{"b"},
		},
		{
			name:        "NULLS DISTINCT",
			sql:         "CREATE TABLE c (a int UNIQUE NULLS DISTINCT);",
			wantUniques: []uniqueOf{{Columns: []string{"a"}, NullsDistinct: &distinct}},
		},
		{
			name:        "a named key",
			sql:         "CREATE TABLE c (a int CONSTRAINT c_a_uq UNIQUE NULLS NOT DISTINCT);",
			wantUniques: []uniqueOf{{Name: "c_a_uq", Columns: []string{"a"}, NullsDistinct: &notDistinct}},
		},
		{
			name:        "a named key without the clause",
			sql:         "CREATE TABLE c (a int CONSTRAINT c_a_uq UNIQUE);",
			wantUniques: []uniqueOf{{Name: "c_a_uq", Columns: []string{"a"}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			uniques, columns := tableUniques(c, test.sql)

			c.Assert(uniques, qt.DeepEquals, test.wantUniques)
			c.Assert(columns, qt.DeepEquals, test.wantColumns)
		})
	}
}

// TestParse_KeyIndexClause_FailurePath refuses by name the clauses PostgreSQL
// takes for the index behind a key, which the model does not keep on one.
// PostgreSQL 18.6 accepts every row. Unrecognized, `USING INDEX TABLESPACE`
// is refused as `unsupported column attribute: TABLESPACE` and `WITH (...)` as
// a missing PARSER, which names neither clause (stokaro/ptah#3821).
func TestParse_KeyIndexClause_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "a table UNIQUE in a tablespace",
			sql:     "CREATE TABLE c (a int, CONSTRAINT u2 UNIQUE (a) USING INDEX TABLESPACE pg_default);",
			wantErr: `UNIQUE USING INDEX TABLESPACE at position 48: the tablespace of the index behind a key is not read; .*`,
		},
		{
			name:    "a table PRIMARY KEY in a tablespace",
			sql:     "CREATE TABLE c (a int, PRIMARY KEY (a) USING INDEX TABLESPACE pg_default);",
			wantErr: `PRIMARY KEY USING INDEX TABLESPACE at position 39: .*`,
		},
		{
			name:    "a column UNIQUE in a tablespace",
			sql:     "CREATE TABLE c (a int UNIQUE USING INDEX TABLESPACE pg_default);",
			wantErr: `UNIQUE USING INDEX TABLESPACE at position 29: .*`,
		},
		{
			name:    "storage parameters after a UNIQUE",
			sql:     "CREATE TABLE c (a int, UNIQUE (a) WITH (fillfactor = 70));",
			wantErr: `UNIQUE WITH \(\.\.\.\) at position 34: storage parameters of the index behind a key are not read; .*`,
		},
		{
			name:    "storage parameters after INCLUDE",
			sql:     "CREATE TABLE c (a int, b int, UNIQUE (a) INCLUDE (b) WITH (fillfactor = 70));",
			wantErr: `UNIQUE WITH \(\.\.\.\) at position 53: .*`,
		},
		{
			name:    "storage parameters after an EXCLUDE",
			sql:     "CREATE TABLE c (r int, EXCLUDE USING btree (r WITH =) WITH (fillfactor = 70));",
			wantErr: `EXCLUDE WITH \(\.\.\.\) at position 54: .*`,
		},
		{
			name:    "storage parameters after a column PRIMARY KEY",
			sql:     "CREATE TABLE c (a int PRIMARY KEY WITH (fillfactor = 70));",
			wantErr: `PRIMARY KEY WITH \(\.\.\.\) at position 34: .*`,
		},
		{
			name:    "a NULLS clause on a column ALTER TABLE adds",
			sql:     "CREATE TABLE c (a int);\nALTER TABLE c ADD COLUMN b int UNIQUE NULLS NOT DISTINCT;",
			wantErr: `UNIQUE NULLS at position 55: a column's UNIQUE with a NULLS clause is read as the table's UNIQUE .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(test.sql, parser.WithDialect(platform.Postgres)).Parse()

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// keyDeferral is what one key of a table carries.
type keyDeferral struct {
	Type       ast.ConstraintType
	Name       string
	Columns    []string
	Deferrable bool
	Initially  string
}

// keysOf lists the PRIMARY KEY, UNIQUE and EXCLUDE constraints the statements
// declare, in order: those of the CREATE TABLE, then those an ALTER TABLE
// adds. It also lists the columns that keep a key of their own.
func keysOf(c *qt.C, sql string) ([]keyDeferral, []string) {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect(platform.Postgres)).Parse()
	c.Assert(err, qt.IsNil)
	var keys []keyDeferral
	var columnKeys []string
	add := func(constraint *ast.ConstraintNode) {
		keys = append(keys, keyDeferral{
			Type: constraint.Type, Name: constraint.Name, Columns: constraint.Columns,
			Deferrable: constraint.Deferrable, Initially: constraint.Initially,
		})
	}
	for _, statement := range statements.Statements {
		switch node := statement.(type) {
		case *ast.CreateTableNode:
			for _, column := range node.Columns {
				switch {
				case column.Primary:
					columnKeys = append(columnKeys, column.Name+" PRIMARY KEY")
				case column.Unique:
					columnKeys = append(columnKeys, column.Name+" UNIQUE")
				}
			}
			for _, constraint := range node.Constraints {
				add(constraint)
			}
		case *ast.AlterTableNode:
			for _, operation := range node.Operations {
				if added, ok := operation.(*ast.AddConstraintOperation); ok {
					add(added.Constraint)
				}
			}
		}
	}
	return keys, columnKeys
}

// TestParse_KeyDeferral_HappyPath reads the deferral clauses after a PRIMARY
// KEY, UNIQUE or EXCLUDE onto the key. PostgreSQL 18.6 accepts each row, and
// pg_constraint's condeferrable and condeferred say what the row wants. A
// column's key that defers its check is read as the table's key over the
// column; measured, `x int UNIQUE DEFERRABLE` builds `UNIQUE (x) DEFERRABLE`
// under the same name as the table-level spelling.
func TestParse_KeyDeferral_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		wantKeys       []keyDeferral
		wantColumnKeys []string
	}{
		{
			name:     "a deferrable UNIQUE",
			sql:      "CREATE TABLE c (a int, UNIQUE (a) DEFERRABLE);",
			wantKeys: []keyDeferral{{Type: ast.UniqueConstraint, Columns: []string{"a"}, Deferrable: true}},
		},
		{
			name: "a deferred EXCLUDE",
			sql:  "CREATE TABLE c (r int, EXCLUDE USING btree (r WITH =) INITIALLY DEFERRED);",
			wantKeys: []keyDeferral{
				{Type: ast.ExcludeConstraint, Deferrable: true, Initially: "deferred"},
			},
		},
		{
			name: "a deferrable named PRIMARY KEY",
			sql:  "CREATE TABLE c (a int, CONSTRAINT c_pk PRIMARY KEY (a) DEFERRABLE INITIALLY IMMEDIATE);",
			wantKeys: []keyDeferral{
				{Type: ast.PrimaryKeyConstraint, Name: "c_pk", Columns: []string{"a"}, Deferrable: true, Initially: "immediate"},
			},
		},
		{
			name: "a column's deferrable UNIQUE",
			sql:  "CREATE TABLE c (a int UNIQUE DEFERRABLE, b int UNIQUE);",
			wantKeys: []keyDeferral{
				{Type: ast.UniqueConstraint, Columns: []string{"a"}, Deferrable: true},
			},
			wantColumnKeys: []string{"b UNIQUE"},
		},
		{
			name: "a column's deferred PRIMARY KEY",
			sql:  "CREATE TABLE c (a int PRIMARY KEY INITIALLY DEFERRED);",
			wantKeys: []keyDeferral{
				{Type: ast.PrimaryKeyConstraint, Columns: []string{"a"}, Deferrable: true, Initially: "deferred"},
			},
		},
		{
			name: "a column's named deferrable UNIQUE",
			sql:  "CREATE TABLE c (a int CONSTRAINT c_a_uq UNIQUE DEFERRABLE);",
			wantKeys: []keyDeferral{
				{Type: ast.UniqueConstraint, Name: "c_a_uq", Columns: []string{"a"}, Deferrable: true},
			},
		},
		{
			name:           "a column's key that stays on the column",
			sql:            "CREATE TABLE c (a int PRIMARY KEY NOT DEFERRABLE, b int UNIQUE INITIALLY IMMEDIATE);",
			wantColumnKeys: []string{"a PRIMARY KEY", "b UNIQUE"},
		},
		{
			name: "ALTER TABLE adds a deferred UNIQUE",
			sql:  "CREATE TABLE c (a int);\nALTER TABLE c ADD CONSTRAINT u UNIQUE (a) DEFERRABLE INITIALLY DEFERRED;",
			wantKeys: []keyDeferral{
				{Type: ast.UniqueConstraint, Name: "u", Columns: []string{"a"}, Deferrable: true, Initially: "deferred"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			keys, columnKeys := keysOf(c, test.sql)

			c.Assert(keys, qt.DeepEquals, test.wantKeys)
			c.Assert(columnKeys, qt.DeepEquals, test.wantColumnKeys)
		})
	}
}

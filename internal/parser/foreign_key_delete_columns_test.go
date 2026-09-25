package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// foreignKeyReferenceOf returns the reference of the one foreign key the
// statement declares, at table or column level.
func foreignKeyReferenceOf(c *qt.C, sql string) *ast.ForeignKeyRef {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect("postgres")).Parse()
	c.Assert(err, qt.IsNil)
	var references []*ast.ForeignKeyRef
	for _, statement := range statements.Statements {
		switch node := statement.(type) {
		case *ast.CreateTableNode:
			for _, column := range node.Columns {
				if column.ForeignKey != nil {
					references = append(references, column.ForeignKey)
				}
			}
			for _, constraint := range node.Constraints {
				if constraint.Reference != nil {
					references = append(references, constraint.Reference)
				}
			}
		case *ast.AlterTableNode:
			for _, operation := range node.Operations {
				if add, ok := operation.(*ast.AddConstraintOperation); ok && add.Constraint.Reference != nil {
					references = append(references, add.Constraint.Reference)
				}
			}
		}
	}
	c.Assert(references, qt.HasLen, 1)
	return references[0]
}

// The column list PostgreSQL 15 takes after ON DELETE SET NULL and SET DEFAULT
// is read onto the reference. A list was a syntax error here, so the schema
// file that raised stokaro/ptah#3562 could not be read at all.
func TestParse_OnDeleteColumnList_HappyPath(t *testing.T) {
	rows := []struct {
		name         string
		sql          string
		wantOnDelete string
		wantOnUpdate string
		wantColumns  []string
	}{
		{
			name:         "SET NULL on one column of a composite key",
			sql:          "CREATE TABLE c (x int, a int, CONSTRAINT c_fk FOREIGN KEY (x, a) REFERENCES p (a, b) ON DELETE SET NULL (a));",
			wantOnDelete: "SET NULL",
			wantColumns:  []string{"a"},
		},
		{
			name:         "SET DEFAULT",
			sql:          "CREATE TABLE c (x int, a int DEFAULT 0, FOREIGN KEY (x, a) REFERENCES p (a, b) ON DELETE SET DEFAULT (a));",
			wantOnDelete: "SET DEFAULT",
			wantColumns:  []string{"a"},
		},
		{
			name:         "several columns, and ON UPDATE after the list",
			sql:          "CREATE TABLE c (x int, a int, FOREIGN KEY (x, a) REFERENCES p (a, b) ON DELETE SET NULL (x, a) ON UPDATE CASCADE);",
			wantOnDelete: "SET NULL",
			wantOnUpdate: "CASCADE",
			wantColumns:  []string{"x", "a"},
		},
		{
			// The order pg_get_constraintdef prints it in.
			name:         "ON UPDATE before the list",
			sql:          "ALTER TABLE c ADD CONSTRAINT c_fk FOREIGN KEY (x, a) REFERENCES p(a, b) ON UPDATE CASCADE ON DELETE SET NULL (a);",
			wantOnDelete: "SET NULL",
			wantOnUpdate: "CASCADE",
			wantColumns:  []string{"a"},
		},
		{
			// Kept as written, quotes included, as the key's own columns are;
			// the model normalizes both the same way.
			name:         "a quoted column",
			sql:          `CREATE TABLE c ("X" int, "Aa" int, FOREIGN KEY ("X", "Aa") REFERENCES p (a, b) ON DELETE SET NULL ("Aa"));`,
			wantOnDelete: "SET NULL",
			wantColumns:  []string{`"Aa"`},
		},
		{
			name:         "a column-level REFERENCES naming its own column",
			sql:          "CREATE TABLE c (a int REFERENCES p1 (id) ON DELETE SET NULL (a));",
			wantOnDelete: "SET NULL",
			wantColumns:  []string{"a"},
		},
		{
			// An unquoted name folds to lower case on both sides.
			name:         "a column-level list in another case",
			sql:          "CREATE TABLE c (A int REFERENCES p1 (id) ON DELETE SET NULL (a));",
			wantOnDelete: "SET NULL",
			wantColumns:  []string{"a"},
		},
		{
			name:         "no list",
			sql:          "CREATE TABLE c (x int, a int, FOREIGN KEY (x, a) REFERENCES p (a, b) ON DELETE SET NULL);",
			wantOnDelete: "SET NULL",
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			reference := foreignKeyReferenceOf(c, row.sql)

			c.Assert(reference.OnDelete, qt.Equals, row.wantOnDelete)
			c.Assert(reference.OnUpdate, qt.Equals, row.wantOnUpdate)
			c.Assert(reference.OnDeleteColumns, qt.DeepEquals, row.wantColumns)
		})
	}
}

// A list anywhere PostgreSQL does not take one is refused rather than left for
// the next clause to trip over.
func TestParse_OnDeleteColumnList_FailurePath(t *testing.T) {
	rows := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			// PostgreSQL 18.6: "a column list with SET NULL is only supported
			// for ON DELETE actions".
			name:    "after ON UPDATE SET NULL",
			sql:     "CREATE TABLE c (x int, a int, FOREIGN KEY (x, a) REFERENCES p (a, b) ON UPDATE SET NULL (a));",
			wantErr: `a column list after ON UPDATE SET NULL at position \d+: only ON DELETE SET NULL and ON DELETE SET DEFAULT take one`,
		},
		{
			// PostgreSQL 18.6: syntax error at or near "(".
			name:    "after CASCADE",
			sql:     "CREATE TABLE c (x int, a int, FOREIGN KEY (x, a) REFERENCES p (a, b) ON DELETE CASCADE (a));",
			wantErr: `a column list after ON DELETE CASCADE at position \d+: only ON DELETE SET NULL and ON DELETE SET DEFAULT take one`,
		},
		{
			// PostgreSQL 18.6: column "b" referenced in ON DELETE SET action
			// must be part of foreign key.
			name:    "a column-level list naming another column",
			sql:     "CREATE TABLE c (a int REFERENCES p1 (id) ON DELETE SET NULL (b), b int);",
			wantErr: `column a: ON DELETE SET NULL names column b, which is not part of the foreign key; a column-level REFERENCES covers only its own column`,
		},
		{
			name:    "the same check on ALTER TABLE ... ADD COLUMN",
			sql:     "ALTER TABLE c ADD COLUMN a int REFERENCES p1 (id) ON DELETE SET NULL (b);",
			wantErr: `column a: ON DELETE SET NULL names column b, which is not part of the foreign key; .*`,
		},
		{
			// A quoted name keeps its case, so "A" and a are two columns.
			name:    "a quoted column-level name in another case",
			sql:     `CREATE TABLE c ("A" int REFERENCES p1 (id) ON DELETE SET NULL (a));`,
			wantErr: `column "A": ON DELETE SET NULL names column a, which is not part of the foreign key; .*`,
		},
		{
			name:    "an empty list",
			sql:     "CREATE TABLE c (x int, a int, FOREIGN KEY (x, a) REFERENCES p (a, b) ON DELETE SET NULL ());",
			wantErr: `expected column name in ON DELETE SET NULL \(\.\.\.\): .*`,
		},
		{
			name:    "an unterminated list",
			sql:     "CREATE TABLE c (x int, a int, FOREIGN KEY (x, a) REFERENCES p (a, b) ON DELETE SET NULL (a b));",
			wantErr: `expected '\)' after the ON DELETE SET NULL column list: .*`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser(row.sql, parser.WithDialect("postgres")).Parse()

			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

package parser_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/parser"
)

// functionalKeyTable wraps one table-body declaration in a table with columns
// for it to index.
func functionalKeyTable(declaration string) string {
	return fmt.Sprintf(
		"CREATE TABLE t (a INT NOT NULL, b INT NOT NULL, %s);",
		declaration,
	)
}

// TestParse_MySQLFunctionalKeyPart covers the parser half of
// stokaro/ptah#2758.
//
// MySQL takes an expression where a key part's column name goes -- a functional
// key part -- and the table-body reader refused every spelling of it, named and
// unnamed alike, with `expected column name`. The standalone
// `CREATE INDEX idx ON t ((a + 1))` already parsed, so the refusal was the
// table-body column list's alone.
//
// Measured on MySQL 8.4: each of these creates the table, and
// information_schema.STATISTICS reports the expression in EXPRESSION with
// COLUMN_NAME null.
func TestParse_MySQLFunctionalKeyPart(t *testing.T) {
	tests := []struct {
		name        string
		declaration string
		wantParts   int
		wantExpr    string
	}{
		{name: "an unnamed functional key", declaration: "KEY ((a + 1))", wantParts: 1, wantExpr: "a + 1"},
		{name: "a named functional key", declaration: "KEY k ((a + 1))", wantParts: 1, wantExpr: "a + 1"},
		{name: "a column and an expression", declaration: "KEY k (a, (b + 1))", wantParts: 2, wantExpr: "b + 1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			table := parsedTable(c, functionalKeyTable(test.declaration))

			// The expression is what has to survive, not merely the parse. An
			// index that arrived with its parts emptied would satisfy an
			// assertion that only asked whether an error came back -- which is
			// how `UNIQUE KEY u ((a + 1))` reached a rendered
			// `CONSTRAINT u UNIQUE (``)` before it was refused instead.
			c.Assert(table.Indexes, qt.HasLen, 1)
			c.Assert(table.Indexes[0].Parts, qt.HasLen, test.wantParts)
			c.Assert(table.Indexes[0].Parts[test.wantParts-1].Expr, qt.Equals, test.wantExpr)
			c.Assert(table.Indexes[0].Parts[test.wantParts-1].Name, qt.Equals, "")
		})
	}
}

// TestParse_MySQLFunctionalKeyPartIsRefusedWhereTheServerRefusesIt is the
// control the change above cannot be made without.
//
// Teaching a column list to accept an expression is one edit away from
// accepting it everywhere, and MySQL does not. Each row here is measured, and
// the two reasons are different:
//
//   - MariaDB 11.8 has no functional key parts at all and answers
//     `ERROR 1064` to every spelling, so the dialect decides.
//   - MySQL 8.4 accepts them in an index and answers `ERROR 3756`, "the primary
//     key cannot be a functional index", for a PRIMARY KEY. That one is not a
//     dialect difference and would be missed by reasoning from the rows above.
func TestParse_MySQLFunctionalKeyPartIsRefusedWhereTheServerRefusesIt(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		declaration string
	}{
		{name: "mariadb has no functional key parts", dialect: "mariadb", declaration: "KEY ((a + 1))"},
		{name: "mariadb refuses a named one too", dialect: "mariadb", declaration: "KEY k ((a + 1))"},
		{name: "mariadb refuses a unique one", dialect: "mariadb", declaration: "UNIQUE KEY u ((a + 1))"},
		{name: "mysql refuses one in a primary key", dialect: "mysql", declaration: "PRIMARY KEY ((a + 1))"},
		// MySQL accepts this one; Ptah does not yet. A table-body UNIQUE becomes a
		// constraint, which has nowhere to keep an expression, and accepting it
		// rendered `CONSTRAINT u UNIQUE (``)` -- the expression gone and an empty
		// identifier in its place. Refused rather than mangled; stokaro/ptah#2793
		// carries it through as a unique index.
		{name: "a unique constraint has nowhere to keep one", dialect: "mysql", declaration: "UNIQUE KEY u ((a + 1))"},
		{name: "mariadb refuses one in a primary key", dialect: "mariadb", declaration: "PRIMARY KEY ((a + 1))"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(
				functionalKeyTable(test.declaration),
				parser.WithDialect(test.dialect),
			).Parse()

			c.Assert(err, qt.IsNotNil)
			c.Assert(statements, qt.IsNil)
		})
	}
}

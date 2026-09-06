package parser_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// alterAddIndexDocument declares a table and then adds one index to it.
func alterAddIndexDocument(operation string) string {
	return fmt.Sprintf(
		"CREATE TABLE t (a INT NOT NULL, s VARCHAR(64) NOT NULL, c TEXT NOT NULL, g GEOMETRY NOT NULL);\n"+
			"ALTER TABLE t %s;",
		operation,
	)
}

// alterAddedIndex returns the index an ALTER added, and fails when the
// statement produced none.
func alterAddedIndex(c *qt.C, dialect, operation string) *ast.IndexNode {
	c.Helper()
	statements, err := parser.NewParser(alterAddIndexDocument(operation), parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	for _, statement := range statements.Statements {
		alter, ok := statement.(*ast.AlterTableNode)
		if !ok {
			continue
		}
		for _, operation := range alter.Operations {
			added, ok := operation.(*ast.AddIndexOperation)
			if ok {
				return added.Index
			}
		}
	}
	c.Fatalf("no AddIndexOperation for %q", operation)
	return nil
}

// alterOperationKinds names the operations every ALTER TABLE in a document
// carries, in order, so a test can assert which path a statement took without
// branching on the node type itself.
func alterOperationKinds(statements *ast.StatementList) []string {
	kinds := make([]string, 0, len(statements.Statements))
	for _, statement := range statements.Statements {
		alter, ok := statement.(*ast.AlterTableNode)
		if !ok {
			continue
		}
		for _, operation := range alter.Operations {
			kinds = append(kinds, fmt.Sprintf("%T", operation))
		}
	}
	return kinds
}

// TestParse_AlterTableAddIndex covers stokaro/ptah#2778.
//
// `ALTER TABLE t ADD KEY k (b)` fell through the ALTER gate to the column
// parser, which read the index name as a column name and its column list as a
// type. The statement then had no place in the model and disappeared: the
// render exited 0 and printed the table without the index.
//
// `ADD INDEX`, the same operation in MySQL's other spelling, reached
// ClickHouse's data-skipping index parser instead and failed with
// `expected TYPE after the expression`. Two spellings of one statement, one
// silently dropped and one refused for a reason belonging to another engine.
//
// Every operation here is accepted by both MySQL 8.4 and MariaDB 11.8, measured
// rather than read off a manual.
func TestParse_AlterTableAddIndex(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		operation  string
		wantName   string
		wantType   string
		wantUnique bool
	}{
		{name: "mysql ADD KEY", dialect: "mysql", operation: "ADD KEY k (a)", wantName: "k"},
		{name: "mysql ADD INDEX", dialect: "mysql", operation: "ADD INDEX k (a)", wantName: "k"},
		{name: "mariadb ADD KEY", dialect: "mariadb", operation: "ADD KEY k (a)", wantName: "k"},
		{name: "mariadb ADD INDEX", dialect: "mariadb", operation: "ADD INDEX k (a)", wantName: "k"},
		{
			name: "mysql ADD SPATIAL KEY", dialect: "mysql",
			operation: "ADD SPATIAL KEY s (g)", wantName: "s", wantType: "SPATIAL",
		},
		{
			name: "mysql ADD FULLTEXT KEY", dialect: "mysql",
			operation: "ADD FULLTEXT KEY f (c)", wantName: "f", wantType: "FULLTEXT",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			index := alterAddedIndex(c, test.dialect, test.operation)

			c.Assert(index.Name, qt.Equals, test.wantName)
			c.Assert(index.Type, qt.Equals, test.wantType)
			c.Assert(index.Unique, qt.Equals, test.wantUnique)
		})
	}
}

// TestParse_AlterTableAddIndexKeepsItsKeyParts asserts what a name alone does
// not.
//
// An operation that arrived with its columns emptied satisfies every assertion
// above, and the index it renders is over nothing. MySQL keeps a prefix length
// and a direction on an added key -- measured, `ADD KEY k (s(3) DESC)` reports
// SUB_PART 3 and COLLATION D -- so both travel here too.
func TestParse_AlterTableAddIndexKeepsItsKeyParts(t *testing.T) {
	c := qt.New(t)
	index := alterAddedIndex(c, "mysql", "ADD KEY k (s(3) DESC)")

	c.Assert(index.Parts, qt.HasLen, 1)
	c.Assert(index.Parts[0].Name, qt.Equals, "s")
	c.Assert(index.Parts[0].Prefix, qt.Equals, "3")
	c.Assert(index.Parts[0].Desc, qt.IsTrue)
}

// TestParse_AlterTableAddUniqueIsStillAConstraint is the control on the gate
// this change widens.
//
// `ADD UNIQUE KEY` and `ADD CONSTRAINT ... UNIQUE` already reached the model as
// constraints and rendered correctly. Widening the gate to admit KEY is one
// edit away from routing those down the index path as well, which would move a
// uniqueness guarantee out of the constraint list that every comparison reads.
func TestParse_AlterTableAddUniqueIsStillAConstraint(t *testing.T) {
	tests := []struct {
		name      string
		operation string
	}{
		{name: "ADD UNIQUE KEY", operation: "ADD UNIQUE KEY u (a)"},
		{name: "ADD CONSTRAINT UNIQUE", operation: "ADD CONSTRAINT u UNIQUE (a)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(
				alterAddIndexDocument(test.operation),
				parser.WithDialect("mysql"),
			).Parse()
			c.Assert(err, qt.IsNil)

			c.Assert(
				alterOperationKinds(statements),
				qt.DeepEquals,
				[]string{"*ast.AddConstraintOperation"},
			)
		})
	}
}

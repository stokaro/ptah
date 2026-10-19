package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// familyMembers are the engines whose descriptor names PostgreSQL's routine
// grammar and the PostgreSQL family. Their own lexers do not read a continued
// or a U& string as one constant, and a routine of theirs is read with
// PostgreSQL's string rules all the same.
var familyMembers = []string{platform.CockroachDB, platform.YugabyteDB, platform.Spanner}

// TestParse_FamilyMemberRoutineBodies_HappyPath pins that a routine body on
// every member of the PostgreSQL family is read with the string rules of the
// family's head: read with the member's own lexer, the continued string loses
// its second segment and the U& string its whole text.
func TestParse_FamilyMemberRoutineBodies_HappyPath(t *testing.T) {
	bodies := []struct {
		name string
		sql  string
	}{
		{name: "a continued string", sql: "CREATE PROCEDURE p()\nLANGUAGE SQL\nAS 'SELECT '\n'1';"},
		{name: "a Unicode escape string", sql: "CREATE PROCEDURE p()\nLANGUAGE SQL\nAS U&'SELECT 1';"},
	}
	for _, dialect := range familyMembers {
		for _, body := range bodies {
			t.Run(dialect+"/"+body.name, func(t *testing.T) {
				c := qt.New(t)

				statements, err := parser.NewParser(body.sql, parser.WithDialect(dialect)).Parse()

				c.Assert(err, qt.IsNil)
				c.Assert(statements.Statements, qt.HasLen, 1)
				routine, ok := statements.Statements[0].(*ast.PostgresRoutineNode)
				c.Assert(ok, qt.IsTrue)
				c.Assert(routine.Body.SQL, qt.Equals, "SELECT 1")
			})
		}
	}
}

// TestParse_FamilyMemberDoBlocks_HappyPath pins the same rules for a DO
// block, which is read as an anonymous routine of the grammar.
func TestParse_FamilyMemberDoBlocks_HappyPath(t *testing.T) {
	for _, dialect := range familyMembers {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parser.NewParser("DO 'BEGIN '\n'NULL; END';", parser.WithDialect(dialect)).Parse()

			c.Assert(err, qt.IsNil)
			c.Assert(statements.Statements, qt.HasLen, 1)
			block, ok := statements.Statements[0].(*ast.PostgresDoBlockNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(block.Body.SQL, qt.Equals, "BEGIN NULL; END")
		})
	}
}

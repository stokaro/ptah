package parser

// White-box testing required: the nesting is a property of the parsed body, and
// the only exported path to it renders through a node whose SQL text is the
// same either way. Asserting on the statement tree is what separates "the
// nesting exists" from "a consumer happened to find the keyword".

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
)

// TestParsePLpgSQL_ControlFlowCarriesItsStatements pins the shape the body
// model gained.
//
// It was flat: an IF carried its whole text, END IF included, as one statement,
// and asking the parser again about that text returned the same statement. A
// consumer could not descend because there was nothing to descend into
// (stokaro/ptah#2393).
func TestParsePLpgSQL_ControlFlowCarriesItsStatements(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		wantKind   ast.PostgresRoutineStatementKind
		wantNested []ast.PostgresRoutineStatementKind
	}{
		{
			name:     "IF carries its branch",
			body:     "IF a THEN\n  EXECUTE 'x';\nEND IF;",
			wantKind: ast.PostgresRoutineStatementIf,
			wantNested: []ast.PostgresRoutineStatementKind{
				ast.PostgresRoutineStatementExecute,
			},
		},
		{
			name:     "IF carries both branches",
			body:     "IF a THEN\n  PERFORM 1;\nELSE\n  EXECUTE 'x';\nEND IF;",
			wantKind: ast.PostgresRoutineStatementIf,
			wantNested: []ast.PostgresRoutineStatementKind{
				ast.PostgresRoutineStatementPerform,
				ast.PostgresRoutineStatementExecute,
			},
		},
		{
			name:     "LOOP carries its body",
			body:     "LOOP\n  EXECUTE 'x';\nEND LOOP;",
			wantKind: ast.PostgresRoutineStatementLoop,
			wantNested: []ast.PostgresRoutineStatementKind{
				ast.PostgresRoutineStatementExecute,
			},
		},
		{
			name:     "FOR carries its body",
			body:     "FOR r IN SELECT 1 LOOP\n  PERFORM 2;\nEND LOOP;",
			wantKind: ast.PostgresRoutineStatementLoop,
			wantNested: []ast.PostgresRoutineStatementKind{
				ast.PostgresRoutineStatementPerform,
			},
		},
		{
			// The control: a statement that carries nothing must carry nothing,
			// or "has nested statements" would mean nothing.
			name:       "a plain statement carries nothing",
			body:       "PERFORM 1;",
			wantKind:   ast.PostgresRoutineStatementPerform,
			wantNested: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements := parsePostgresPLpgSQLStatements("BEGIN\n" + test.body + "\nEND;")

			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0].Kind, qt.Equals, test.wantKind)
			c.Assert(kindsOf(statements[0].Statements), qt.DeepEquals, test.wantNested)
		})
	}
}

// TestParsePLpgSQL_NestingGoesAllTheWayDown pins that a statement inside a
// statement inside a statement is reached.
func TestParsePLpgSQL_NestingGoesAllTheWayDown(t *testing.T) {
	c := qt.New(t)

	statements := parsePostgresPLpgSQLStatements(
		"BEGIN\n  IF a THEN\n    LOOP\n      EXECUTE 'x';\n    END LOOP;\n  END IF;\nEND;")

	c.Assert(statements, qt.HasLen, 1)
	loops := statements[0].Statements
	c.Assert(kindsOf(loops), qt.DeepEquals,
		[]ast.PostgresRoutineStatementKind{ast.PostgresRoutineStatementLoop})
	c.Assert(kindsOf(loops[0].Statements), qt.DeepEquals,
		[]ast.PostgresRoutineStatementKind{ast.PostgresRoutineStatementExecute})
}

// kindsOf lists the kinds of a statement list, nil when it is empty.
func kindsOf(statements []ast.PostgresRoutineStatement) []ast.PostgresRoutineStatementKind {
	if len(statements) == 0 {
		return nil
	}
	kinds := make([]ast.PostgresRoutineStatementKind, 0, len(statements))
	for _, statement := range statements {
		kinds = append(kinds, statement.Kind)
	}
	return kinds
}

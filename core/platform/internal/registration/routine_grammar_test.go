package registration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// parsedStatement parses sql as dialect and returns its one statement.
func parsedStatement(c *qt.C, sql, dialect string) ast.Node {
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	return statements.Statements[0]
}

// TestRegister_GivesADoBlockTheDefaultLanguageOfAnEngineNobodyShips registers
// two engines that read routines in the quoted-body grammar, one stating the
// language of a DO block that writes none and one stating no language. Before
// the registration a DO block is raw SQL. After it a block that writes no
// LANGUAGE has the stated language, or none where the engine states none, and
// a block that writes one has that one on both.
func TestRegister_GivesADoBlockTheDefaultLanguageOfAnEngineNobodyShips(t *testing.T) {
	const unwritten = "DO $$ BEGIN NULL; END $$;"
	const written = "DO LANGUAGE PLV8 $$ BEGIN NULL; END $$;"
	c := qt.New(t)
	raw, isRaw := parsedStatement(c, unwritten, "blockdb").(*ast.RawSQLNode)
	c.Assert(isRaw, qt.IsTrue)
	c.Assert(raw.SQL, qt.Equals, unwritten)

	stated := described("blocking", "", "blockdb")
	stated.Grammar.Routines = descriptor.RoutineGrammar{Body: descriptor.RoutineQuotedBody, DefaultDoBlockLanguage: "plblock"}
	c.Assert(platform.Register(stated), qt.IsNil)
	unstated := described("unstated", "", "unstateddb")
	unstated.Grammar.Routines = descriptor.RoutineGrammar{Body: descriptor.RoutineQuotedBody}
	c.Assert(platform.Register(unstated), qt.IsNil)

	tests := []struct {
		name, sql, dialect, wantLanguage string
	}{
		{name: "no LANGUAGE on an engine that states one", sql: unwritten, dialect: "blockdb", wantLanguage: "plblock"},
		{name: "a LANGUAGE on an engine that states one", sql: written, dialect: "blockdb", wantLanguage: "plv8"},
		{name: "no LANGUAGE on an engine that states none", sql: unwritten, dialect: "unstateddb", wantLanguage: ""},
		{name: "a LANGUAGE on an engine that states none", sql: written, dialect: "unstateddb", wantLanguage: "plv8"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			block, isBlock := parsedStatement(c, test.sql, test.dialect).(*ast.DoBlockNode)

			c.Assert(isBlock, qt.IsTrue)
			c.Assert(block.Language, qt.Equals, test.wantLanguage)
			c.Assert(block.Body, qt.Equals, ast.RoutineBody{SQL: " BEGIN NULL; END ", Delimiter: "$$", Language: test.wantLanguage})
		})
	}
}

// TestRegister_ReadsTheRoutinesOfAnEngineNobodyShipsInTheGrammarItStates
// registers an engine whose descriptor states the compound-statement routine
// grammar. Before the registration a procedure is raw SQL, which the SQL
// reader keeps out of the schema. After it the procedure is a routine node
// with its header read, and the SQL reader converts it as it converts a
// MySQL procedure: the routine grammar the descriptor states decides, not the
// engine's name.
func TestRegister_ReadsTheRoutinesOfAnEngineNobodyShipsInTheGrammarItStates(t *testing.T) {
	const procedure = "CREATE PROCEDURE touch(IN n INT) DETERMINISTIC SQL SECURITY INVOKER COMMENT 'tick' BEGIN SELECT n; END;"
	c := qt.New(t)
	_, isRaw := parsedStatement(c, procedure, "compounddb").(*ast.RawSQLNode)
	c.Assert(isRaw, qt.IsTrue)
	database, _, err := sqlschema.Read([]byte(procedure), "compounddb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 0)

	engine := described("compound", "", "compounddb")
	engine.Grammar.Routines = descriptor.RoutineGrammar{Body: descriptor.RoutineCompoundStatement}
	c.Assert(platform.Register(engine), qt.IsNil)

	routine, isRoutine := parsedStatement(c, procedure, "compounddb").(*ast.RoutineNode)
	c.Assert(isRoutine, qt.IsTrue)
	c.Assert(routine.Dialect, qt.Equals, "compound")
	c.Assert(routine.Kind, qt.Equals, ast.RoutineKindProcedure)
	c.Assert(routine.Name, qt.Equals, "touch")
	c.Assert(routine.Options, qt.DeepEquals, []ast.RoutineOption{
		{Text: "DETERMINISTIC"}, {Text: "SQL SECURITY INVOKER"}, {Text: "COMMENT 'tick'"},
	})
	c.Assert(routine.Comment, qt.Equals, "tick")
	c.Assert(routine.Body, qt.Equals, ast.RoutineBody{SQL: "BEGIN SELECT n; END"})
	database, _, err = sqlschema.Read([]byte(procedure), "compounddb")
	c.Assert(err, qt.IsNil)
	mysql, _, err := sqlschema.Read([]byte(procedure), platform.MySQL)
	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 1)
	c.Assert(database.Functions, qt.DeepEquals, mysql.Functions)
	c.Assert(database.Functions[0], qt.DeepEquals, schemamodel.Function{
		Name: "touch", Kind: "procedure", Parameters: "in n int", Language: "sql",
		Security: "INVOKER", Volatility: "IMMUTABLE", Body: "BEGIN SELECT n; END", Comment: "tick",
	})
}

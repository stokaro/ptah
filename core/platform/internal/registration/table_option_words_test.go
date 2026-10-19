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

// parsedOptions parses one CREATE TABLE as dialect and returns its options.
func parsedOptions(c *qt.C, sql, dialect string) map[string]string {
	statements, err := parser.NewParser(sql, parser.WithDialect(dialect)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	table, isTable := statements.Statements[0].(*ast.CreateTableNode)
	c.Assert(isTable, qt.IsTrue)
	return table.Options
}

// TestRegister_ReadsTheTableOptionWordsOfAnEngineNobodyShips registers an
// engine whose descriptor spells two table options with the same first word,
// APPEND ONLY and Append Log, and a third with a word the shared grammar
// reads itself, TABLESPACE. Before the registration the parser refuses
// APPEND. After it every dialect reads each option under the name the
// descriptor gives it, and so does a parse with no dialect: the option whose
// words follow is the one read, in any case. The SQL reader states it as a
// property of the engine's group. TABLESPACE is still read as the shared
// grammar's.
func TestRegister_ReadsTheTableOptionWordsOfAnEngineNobodyShips(t *testing.T) {
	const appendOnly = "CREATE TABLE events (id int) APPEND ONLY;"
	const appendLog = "CREATE TABLE events (id int) append /* log */ log;"
	const tablespace = "CREATE TABLE events (id int) TABLESPACE fast;"
	c := qt.New(t)
	statements, err := parser.NewParser(appendOnly, parser.WithDialect("appenddb")).Parse()
	c.Assert(err, qt.ErrorMatches, `unsupported table option: APPEND at position 29`)
	c.Assert(statements, qt.IsNil)
	database, _, err := sqlschema.Read([]byte(appendOnly), "appenddb")
	c.Assert(err, qt.ErrorMatches, `unsupported table option: APPEND at position 29`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})

	engine := described("appending", "", "appenddb")
	engine.TableOptions = []descriptor.TableOption{
		{Option: "APPEND_ONLY", Words: []string{"APPEND", "ONLY"}, Property: "append_only"},
		{Option: "JOURNALED", Words: []string{"Append", "Log"}, Property: "append_log"},
		{Option: "TABLESPACE_FLAG", Words: []string{"TABLESPACE"}, Property: "tablespace_flag"},
	}
	c.Assert(platform.Register(engine), qt.IsNil)

	for _, dialect := range []string{"appenddb", platform.Postgres, platform.SQLite, ""} {
		c.Assert(parsedOptions(c, appendOnly, dialect), qt.DeepEquals, map[string]string{"APPEND_ONLY": "true"})
		c.Assert(parsedOptions(c, appendLog, dialect), qt.DeepEquals, map[string]string{"JOURNALED": "true"})
		c.Assert(parsedOptions(c, tablespace, dialect), qt.DeepEquals, map[string]string{"TABLESPACE": "fast"})
	}
	database, _, err = sqlschema.Read([]byte(appendOnly), "appenddb")
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		"appending": {"append_only": "true"},
	})
	database, _, err = sqlschema.Read([]byte(appendLog), platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{
		"appending": {"append_log": "true"},
	})
}

// TestRegister_RefusesAnIncompleteTableOptionOfAnEngineNobodyShips registers
// an engine that spells two table options with the same first word. A
// document that writes the first word and neither second word is refused,
// naming the first option's second word, on the engine and on a parse with no
// dialect.
func TestRegister_RefusesAnIncompleteTableOptionOfAnEngineNobodyShips(t *testing.T) {
	const journalMode = "CREATE TABLE events (id int) JOURNAL MODE;"
	c := qt.New(t)
	engine := described("journaling", "", "journaldb")
	engine.TableOptions = []descriptor.TableOption{
		{Option: "JOURNAL_ONLY", Words: []string{"JOURNAL", "ONLY"}, Property: "journal_only"},
		{Option: "JOURNAL_ALWAYS", Words: []string{"JOURNAL", "ALWAYS"}, Property: "journal_always"},
	}
	c.Assert(platform.Register(engine), qt.IsNil)

	for _, dialect := range []string{"journaldb", ""} {
		statements, err := parser.NewParser(journalMode, parser.WithDialect(dialect)).Parse()
		c.Assert(err, qt.ErrorMatches, `expected ONLY after JOURNAL: expected 'ONLY', got 'MODE' at position 37`)
		c.Assert(statements, qt.IsNil)
	}
	database, _, err := sqlschema.Read([]byte(journalMode), "journaldb")
	c.Assert(err, qt.ErrorMatches, `expected ONLY after JOURNAL: expected 'ONLY', got 'MODE' at position 37`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
}

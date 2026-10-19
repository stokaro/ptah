package registration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/sqlcompound"
	"ptah.run/internal/sqlreach"
)

// TestRegister_DescribesTheTextOfAnEngineNobodyShips registers an engine with
// lexical, body and reach facts of its own. The lexer configuration, the body
// splitter and the reach scan read them, though nothing names the engine.
// Each is asked first, so the answers derived from the earlier registry exist
// and have to give way to the new one.
func TestRegister_DescribesTheTextOfAnEngineNobodyShips(t *testing.T) {
	const fetch = "SELECT * FROM fetch_url('http://169.254.169.254/')"
	c := qt.New(t)
	c.Assert(dialectlexer.Options("lexdb"), qt.DeepEquals, lexer.Options{StandardStrings: true})
	c.Assert(sqlreach.DialectUsesBackslashEscapes("lexdb"), qt.IsFalse)
	_, found, err := sqlreach.Scan(fetch, "lexdb")
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsFalse)
	c.Assert(keepsSemicolonAfterAS("lexdb"), qt.IsFalse)

	engine := described("lexical", "", "lexdb")
	engine.Lexical = descriptor.Lexical{BackslashEscapes: true, BracketIdentifiers: true}
	engine.Scripts.Bodies = descriptor.BodiesTransactSQL
	engine.Reach.TableFunctions = descriptor.ReachCalls{
		Construct: "lexical remote table function", Reach: "reads a URL the statement names", Names: []string{"FETCH_URL"},
	}
	c.Assert(platform.Register(engine), qt.IsNil)

	c.Assert(dialectlexer.Options("lexdb"), qt.DeepEquals, lexer.Options{
		StandardStrings: true, BackslashEscapes: true, BracketIdentifiers: true,
	})
	c.Assert(sqlreach.DialectUsesBackslashEscapes("lexdb"), qt.IsTrue)
	finding, found, err := sqlreach.Scan(fetch, "lexdb")
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(finding, qt.DeepEquals, sqlreach.Finding{
		Construct: "lexical remote table function", Reach: "reads a URL the statement names",
	})
	c.Assert(keepsSemicolonAfterAS("lexdb"), qt.IsTrue)
}

// keepsSemicolonAfterAS reports whether the body splitter of dialect reads
// `CREATE PROCEDURE p AS` as the start of a body, as Transact-SQL does.
func keepsSemicolonAfterAS(dialect string) bool {
	state := sqlcompound.New(dialect)
	for _, word := range []string{"CREATE", "PROCEDURE", "p", "AS"} {
		state.Word(word)
	}
	return state.KeepSemicolonInsideStatement()
}

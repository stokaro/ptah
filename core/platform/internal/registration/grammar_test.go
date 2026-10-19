package registration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/parser"
)

// TestRegister_DescribesTheDDLGrammarOfAnEngineNobodyShips registers an engine
// with grammar facts of its own, and the shared DDL parser reads its documents
// by them, though nothing names the engine. The parser is asked first, so the
// grammar derived from the earlier registry exists and has to give way to the
// new one.
func TestRegister_DescribesTheDDLGrammarOfAnEngineNobodyShips(t *testing.T) {
	const twoChecks = "CREATE TABLE t (a int CHECK (a > 0) CHECK (a < 10));"
	const deferred = "CREATE TABLE t (id int PRIMARY KEY, p int REFERENCES t (id) DEFERRABLE);"
	c := qt.New(t)
	_, err := parser.NewParser(twoChecks, parser.WithDialect("grammardb")).Parse()
	c.Assert(err, qt.IsNil)
	_, err = parser.NewParser(deferred, parser.WithDialect("grammardb")).Parse()
	c.Assert(err, qt.IsNil)

	engine := described("grammar", "", "grammardb")
	engine.Grammar = descriptor.Grammar{
		Columns: descriptor.ColumnGrammar{SecondCheckRefusal: "grammar takes one CHECK on a column"},
		Constraints: descriptor.ConstraintGrammar{
			Deferral: descriptor.Deferral{Refusal: "grammar has no deferrable constraints"},
		},
	}
	c.Assert(platform.Register(engine), qt.IsNil)

	statements, err := parser.NewParser(twoChecks, parser.WithDialect("grammardb")).Parse()
	c.Assert(err, qt.ErrorMatches,
		`column a carries a second CHECK at position 36: grammar takes one CHECK on a column`)
	c.Assert(statements, qt.IsNil)
	statements, err = parser.NewParser(deferred, parser.WithDialect("grammardb")).Parse()
	c.Assert(err, qt.ErrorMatches, `deferral clause at position 60: grammar has no deferrable constraints`)
	c.Assert(statements, qt.IsNil)
}

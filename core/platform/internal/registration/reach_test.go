package registration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/sqlreach"
)

// TestRegister_RefusesTheEverywhereConstructOfAnEngineNobodyShips registers
// an engine that states a construct of its own for every engine: EXPORT TO a
// location reaches outside the database. Before the registration PostgreSQL
// and SQLite accept the statement, since no engine states the construct.
// After it both refuse it, under the words the engine states, and so does the
// engine itself, though nothing names the engine.
func TestRegister_RefusesTheEverywhereConstructOfAnEngineNobodyShips(t *testing.T) {
	const statement = "EXPORT TO 's3://bucket/dump' SELECT * FROM t"
	dialects := []string{platform.Postgres, platform.SQLite, "exportdb"}
	c := qt.New(t)
	for _, dialect := range dialects {
		finding, found, err := sqlreach.Scan(statement, dialect)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsFalse)
		c.Assert(finding, qt.DeepEquals, sqlreach.Finding{})
	}

	engine := described("exporting", "", "exportdb")
	engine.Reach.Everywhere = []descriptor.ReachConstruct{{
		Construct:   "EXPORT TO",
		Reach:       "writes the result of a query to a location outside the database",
		StartsWith:  []string{"EXPORT", "TO"},
		StringAfter: []string{"TO"},
	}}
	c.Assert(platform.Register(engine), qt.IsNil)

	want := sqlreach.Finding{Construct: "EXPORT TO", Reach: "writes the result of a query to a location outside the database"}
	for _, dialect := range dialects {
		finding, found, err := sqlreach.Scan(statement, dialect)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.IsTrue)
		c.Assert(finding, qt.DeepEquals, want)
	}
}

package postgres

// White-box testing required: readTriggersForSchema is package-local and the
// field under test is filled from a column of its own query, which no exported
// API exposes on its own.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbschema/dbtest"
)

// TestReadTriggersForSchema_ReportsTheFunctionItRuns pins the name beside the
// source.
//
// PostgreSQL has no inline trigger body: a trigger always names a function, and
// Ptah writes one per trigger when the declaration carries a body. The query
// asked for p.prosrc and never p.proname, so the identity of the function was
// discarded at the catalog and every trigger looked like one Ptah owns -- one
// audit function shared by two tables was described twice as two inline bodies
// and replayed as two copies (stokaro/ptah#2210).
func TestReadTriggersForSchema_ReportsTheFunctionItRuns(t *testing.T) {
	c := qt.New(t)

	db := dbtest.Open(t, answeringTriggerFunctions)
	reader := NewPostgreSQLReader(db.SQL, "public")

	triggers, err := reader.readTriggersForSchema(t.Context(), "public")

	c.Assert(err, qt.IsNil)
	c.Assert(triggers, qt.HasLen, 2)
	// Both triggers run the same function. That is the fact the read has to
	// carry: with only the source, the two are indistinguishable from two
	// triggers that happen to have identical bodies.
	c.Assert(triggers[0].ExecuteFunction, qt.Equals, "audit_fn")
	c.Assert(triggers[1].ExecuteFunction, qt.Equals, "audit_fn")
	c.Assert(triggers[0].Body, qt.Contains, "INSERT INTO audit_log")
}

// answeringTriggerFunctions is two triggers on two tables running one function.
//
// It answers what the QUERY asks for, not a fixed shape. A fake that returns the
// same columns whatever it was handed cannot tell a projection that dropped a
// column from one that kept it: the scan still gets as many values as it has
// destinations, and a mutant removing `p.proname` passes as killed. Measured --
// it did, until this branched on the query text.
func answeringTriggerFunctions(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	body := "BEGIN INSERT INTO audit_log(msg) VALUES (TG_TABLE_NAME); RETURN NEW; END"
	if !strings.Contains(query, "p.proname") {
		return dbtest.QueryResult{
			Columns: []string{
				"schema_name", "table_name", "trigger_name",
				"timing", "event", "for_each", "body", "comment",
			},
			Rows: [][]driver.Value{
				{"public", "a", "trg_a", "AFTER", "INSERT", "ROW", body, ""},
				{"public", "b", "trg_b", "AFTER", "INSERT", "ROW", body, ""},
			},
		}, nil
	}
	return dbtest.QueryResult{
		Columns: []string{
			"schema_name", "table_name", "trigger_name",
			"timing", "event", "for_each", "body", "execute_function", "comment",
		},
		Rows: [][]driver.Value{
			{"public", "a", "trg_a", "AFTER", "INSERT", "ROW", body, "audit_fn", ""},
			{"public", "b", "trg_b", "AFTER", "INSERT", "ROW", body, "audit_fn", ""},
		},
	}, nil
}

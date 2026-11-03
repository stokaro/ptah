package mssql

// White-box testing required: readTriggers is package-local and the three facts
// under test are filled from columns of its own query, which no exported API
// exposes on their own.

import (
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbschema/dbtest"
)

// triggersByName reads the fake catalog and returns its triggers by name.
func triggersByName(c *qt.C, answer dbtest.QueryHandler) map[string]catalog.Trigger {
	c.Helper()
	db := dbtest.Open(c, answer)
	reader := NewSQLServerReader(db.SQL, "dbo")

	triggers, err := reader.readTriggers(c.Context())
	c.Assert(err, qt.IsNil)

	byName := make(map[string]catalog.Trigger, len(triggers))
	for _, trigger := range triggers {
		byName[trigger.Name] = trigger
	}
	return byName
}

// TestReadTriggers_ReportsTheTimingAndEventTheCatalogHolds pins the two facts
// a constant would lose.
//
// Assigned Timing = "AFTER" and Event = "" whatever the catalog says, an
// INSTEAD OF DELETE trigger and an AFTER UPDATE one read identically, as AFTER
// INSERT, because the renderer defaults an empty event. That is not a
// spelling difference: an INSTEAD OF trigger replaces the statement and an
// AFTER one runs behind it (stokaro/ptah#2206).
func TestReadTriggers_ReportsTheTimingAndEventTheCatalogHolds(t *testing.T) {
	tests := []struct {
		name        string
		trigger     string
		wantTiming  string
		wantEvent   string
		wantForEach string
	}{
		{name: "an AFTER trigger", trigger: "trg_after", wantTiming: "AFTER", wantEvent: "UPDATE", wantForEach: "STATEMENT"},
		// The control that separates reading the column from swapping one
		// constant for another.
		{name: "an INSTEAD OF trigger", trigger: "trg_instead", wantTiming: "INSTEAD OF", wantEvent: "DELETE", wantForEach: "STATEMENT"},
		{name: "a trigger on two events", trigger: "trg_multi", wantTiming: "AFTER", wantEvent: "INSERT OR UPDATE", wantForEach: "STATEMENT"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			trigger, ok := triggersByName(c, answeringTriggerFacts)[test.trigger]

			c.Assert(ok, qt.IsTrue)
			c.Assert(trigger.Timing, qt.Equals, test.wantTiming)
			c.Assert(trigger.Event, qt.Equals, test.wantEvent)
			c.Assert(trigger.ForEach, qt.Equals, test.wantForEach)
		})
	}
}

// TestReadTriggers_ReportsTheBodyWithoutItsHeader pins that Body is the body.
//
// OBJECT_DEFINITION hands back the whole CREATE TRIGGER statement and Body is
// the body alone. Kept whole, the statement is wrapped in another CREATE
// TRIGGER, and SQL Server refuses the result with `Incorrect syntax near the
// keyword 'TRIGGER'`.
func TestReadTriggers_ReportsTheBodyWithoutItsHeader(t *testing.T) {
	c := qt.New(t)

	trigger, ok := triggersByName(c, answeringTriggerFacts)["trg_after"]

	c.Assert(ok, qt.IsTrue)
	c.Assert(trigger.Body, qt.Equals, "BEGIN INSERT INTO dbo.audit_log(msg) VALUES('u'); END")
	c.Assert(strings.ToUpper(trigger.Body), qt.Not(qt.Contains), "CREATE TRIGGER")
}

// answeringTriggerFacts is three triggers, answering what the query asks for.
//
// A fake returning a fixed shape cannot tell a projection that dropped a column
// from one that kept it: the scan still gets as many values as it has
// destinations, so a mutant removing the timing column would pass as killed.
func answeringTriggerFacts(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	if !strings.Contains(query, "is_instead_of_trigger") || !strings.Contains(query, "trigger_events") {
		return dbtest.QueryResult{
			Columns: []string{"schema_name", "trigger_name", "table_name", "definition"},
			Rows: [][]driver.Value{
				{"dbo", "trg_after", "t", "CREATE TRIGGER dbo.trg_after ON dbo.t AFTER UPDATE AS BEGIN INSERT INTO dbo.audit_log(msg) VALUES('u'); END"},
			},
		}, nil
	}
	return dbtest.QueryResult{
		Columns: []string{"schema_name", "trigger_name", "table_name", "definition", "timing", "event"},
		Rows: [][]driver.Value{
			{
				"dbo", "trg_after", "t",
				"CREATE TRIGGER dbo.trg_after ON dbo.t AFTER UPDATE AS BEGIN INSERT INTO dbo.audit_log(msg) VALUES('u'); END",
				"AFTER", "UPDATE",
			},
			{
				"dbo", "trg_instead", "t",
				"CREATE TRIGGER dbo.trg_instead ON dbo.t INSTEAD OF DELETE AS BEGIN INSERT INTO dbo.audit_log(msg) VALUES('d'); END",
				"INSTEAD OF", "DELETE",
			},
			{
				"dbo", "trg_multi", "t",
				"CREATE TRIGGER dbo.trg_multi ON dbo.t AFTER INSERT, UPDATE AS BEGIN INSERT INTO dbo.audit_log(msg) VALUES('m'); END",
				"AFTER", "INSERT OR UPDATE",
			},
		},
	}, nil
}

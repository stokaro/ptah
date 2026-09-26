package sqlschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

// triggerClausesSchema declares each trigger form stokaro/ptah#3674 reports
// the SQL reader refusing, and the clauses PostgreSQL 18 adds to them.
const triggerClausesSchema = `CREATE TABLE x (id int PRIMARY KEY, a int, b int, "Total" int);
CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER t_events BEFORE UPDATE OF "Total", B OR INSERT ON x FOR EACH ROW EXECUTE FUNCTION f();
CREATE TRIGGER t_truncate AFTER TRUNCATE ON x FOR STATEMENT EXECUTE FUNCTION f();
CREATE TRIGGER t_when BEFORE UPDATE ON x FOR EACH ROW WHEN (NEW.a IS DISTINCT FROM OLD.a) EXECUTE FUNCTION f();
CREATE TRIGGER t_tables AFTER UPDATE ON x REFERENCING OLD TABLE AS "Old Rows" NEW TABLE NewRows
  FOR EACH STATEMENT EXECUTE FUNCTION f();`

func postgresTriggerClausesDatabase(c *qt.C) schemamodel.Database {
	statements, err := parser.NewParser(triggerClausesSchema, parser.WithDialect(platform.Postgres)).Parse()
	c.Assert(err, qt.IsNil)
	database, err := sqlschema.ToDatabase(statements, platform.Postgres)
	c.Assert(err, qt.IsNil)
	schemamodel.Finalize(&database)
	return database
}

// triggerClauses is the part of a modeled trigger the SQL reader fills from the
// clauses it reads.
type triggerClauses struct {
	Name     string
	Event    string
	ForEach  string
	When     string
	OldTable string
	NewTable string
}

// TestToDatabase_PostgresTriggerClausesReachTheModel keeps every clause the
// parser read. The events are in the order PostgreSQL reports them, a column
// keeps its quotes so it still names the column it named, and a transition
// table is named the way the server folds it.
func TestToDatabase_PostgresTriggerClausesReachTheModel(t *testing.T) {
	c := qt.New(t)

	database := postgresTriggerClausesDatabase(c)
	got := make([]triggerClauses, len(database.Triggers))
	for i, trigger := range database.Triggers {
		got[i] = triggerClauses{
			Name: trigger.Name, Event: trigger.Event, ForEach: trigger.ForEach,
			When: trigger.When, OldTable: trigger.OldTable, NewTable: trigger.NewTable,
		}
	}

	c.Assert(got, qt.DeepEquals, []triggerClauses{
		{Name: "t_events", Event: `INSERT OR UPDATE OF "Total", B`, ForEach: "ROW"},
		{Name: "t_truncate", Event: "TRUNCATE", ForEach: "STATEMENT"},
		{Name: "t_when", Event: "UPDATE", ForEach: "ROW", When: "NEW.a IS DISTINCT FROM OLD.a"},
		{Name: "t_tables", Event: "UPDATE", ForEach: "STATEMENT", OldTable: "Old Rows", NewTable: "newrows"},
	})
}

// TestToDatabase_PostgresTriggerClausesRenderAndReadBack renders the model and
// reads the result again: a schema Ptah writes out has to be one it can read.
func TestToDatabase_PostgresTriggerClausesRenderAndReadBack(t *testing.T) {
	c := qt.New(t)

	database := postgresTriggerClausesDatabase(c)
	statements, err := renderer.GetOrderedCreateStatements(&database, platform.Postgres)
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")

	c.Assert(rendered, qt.Contains,
		`CREATE TRIGGER "t_events" BEFORE INSERT OR UPDATE OF "Total", B ON "x" FOR EACH ROW EXECUTE FUNCTION "f"();`)
	c.Assert(rendered, qt.Contains,
		`CREATE TRIGGER "t_truncate" AFTER TRUNCATE ON "x" FOR EACH STATEMENT EXECUTE FUNCTION "f"();`)
	c.Assert(rendered, qt.Contains,
		`CREATE TRIGGER "t_when" BEFORE UPDATE ON "x" FOR EACH ROW WHEN (NEW.a IS DISTINCT FROM OLD.a) EXECUTE FUNCTION "f"();`)
	c.Assert(rendered, qt.Contains,
		`CREATE TRIGGER "t_tables" AFTER UPDATE ON "x" REFERENCING OLD TABLE AS "Old Rows" NEW TABLE AS "newrows" `+
			`FOR EACH STATEMENT EXECUTE FUNCTION "f"();`)

	reread, err := parser.NewParser(rendered, parser.WithDialect(platform.Postgres)).Parse()
	c.Assert(err, qt.IsNil)
	again, err := sqlschema.ToDatabase(reread, platform.Postgres)
	c.Assert(err, qt.IsNil)
	schemamodel.Finalize(&again)
	c.Assert(again.Triggers, qt.DeepEquals, database.Triggers)
}

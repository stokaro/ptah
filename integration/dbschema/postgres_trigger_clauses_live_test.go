//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

// triggerClausesDocument declares a trigger of each form PostgreSQL 18 accepts
// and the SQL reader refused (stokaro/ptah#3674), in a schema of its own.
func triggerClausesDocument(schemaName string) string {
	q := `"` + schemaName + `".`
	return `CREATE TABLE ` + q + `x (id int PRIMARY KEY, a int, b int, "Total" int);
CREATE FUNCTION ` + q + `f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER t_events BEFORE UPDATE OF "Total", B OR INSERT ON ` + q + `x FOR EACH ROW EXECUTE FUNCTION ` + q + `f();
CREATE TRIGGER t_truncate AFTER TRUNCATE ON ` + q + `x FOR STATEMENT EXECUTE FUNCTION ` + q + `f();
CREATE TRIGGER t_when BEFORE UPDATE ON ` + q + `x FOR EACH ROW WHEN (NEW.a IN (1, 2)) EXECUTE FUNCTION ` + q + `f();
CREATE TRIGGER t_tables AFTER UPDATE ON ` + q + `x REFERENCING OLD TABLE AS "Old Rows" NEW TABLE NewRows
  FOR EACH STATEMENT EXECUTE FUNCTION ` + q + `f();`
}

// readTrigger is what the reader reports for one trigger, reduced to the
// clauses this test is about.
type readTrigger struct {
	Name, Event, ForEach, When, OldTable, NewTable string
}

// TestPostgresLiveTriggerClausesConverge creates each trigger form from the
// statements Ptah renders for it, reads the server back, and compares.
//
// Only a server answers each step: whether it accepts the rendered event list,
// condition and transition tables; how it reports them, which is the
// definition pg_get_triggerdef prints rather than what was written; and
// whether the declaration and that report compare as one trigger. The WHEN
// row is the one folding cannot settle: the server prints `NEW.a IN (1, 2)` as
// `(new.a = ANY (ARRAY[1, 2]))`, so the comparison has to ask it.
func TestPostgresLiveTriggerClausesConverge(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_trigclauses_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	declared, _, err := sqlschema.Read([]byte(triggerClausesDocument(schemaName)), platform.Postgres)
	c.Assert(err, qt.IsNil)
	statements, err := renderer.GetOrderedCreateStatements(&declared, platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	got := make([]readTrigger, len(live.Triggers))
	for i, trigger := range live.Triggers {
		got[i] = readTrigger{
			Name: trigger.Name, Event: trigger.Event, ForEach: trigger.ForEach,
			When: trigger.When, OldTable: trigger.OldTable, NewTable: trigger.NewTable,
		}
	}
	c.Assert(got, qt.ContentEquals, []readTrigger{
		{Name: "t_events", Event: `INSERT OR UPDATE OF "Total", b`, ForEach: "ROW"},
		{Name: "t_truncate", Event: "TRUNCATE", ForEach: "STATEMENT"},
		{Name: "t_when", Event: "UPDATE", ForEach: "ROW", When: "(new.a = ANY (ARRAY[1, 2]))"},
		{Name: "t_tables", Event: "UPDATE", ForEach: "STATEMENT", OldTable: "Old Rows", NewTable: "newrows"},
	})

	settled, err := schemadiff.CompareWithDatabase(ctx, conn, &declared, live, nil)
	c.Assert(err, qt.IsNil)
	c.Assert(settled.TriggersAdded, qt.HasLen, 0)
	c.Assert(settled.TriggersModified, qt.HasLen, 0)
	c.Assert(settled.TriggersRemoved, qt.HasLen, 0)

	// The control: without the server's answer, the condition is compared as
	// folded text, and `NEW.a IN (1, 2)` is not `(new.a = ANY (ARRAY[1, 2]))`.
	// So the comparison above converged because it asked the server.
	folded := schemadiff.CompareWithDialect(&declared, live, platform.Postgres)
	c.Assert(folded.TriggersModified, qt.HasLen, 1)
	c.Assert(folded.TriggersModified[0].TriggerName, qt.Equals, "t_when")
}

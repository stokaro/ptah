//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/root"
)

// triggerClausesTable is the table and function every schema below puts a
// trigger on.
const triggerClausesTable = `CREATE TABLE x (id int PRIMARY KEY, a int, b int, "Total" int);
CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
`

// writeTriggerSchema writes the table, the function and one trigger to a
// schema file of their own.
func writeTriggerSchema(c *qt.C, trigger string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(triggerClausesTable+trigger+"\n"), 0o600), qt.IsNil)
	return path
}

// runPtahNativeStdout runs one native command in-process and returns what it
// wrote to standard output alone, which is the machine-clean stream a schema
// file can be written from.
func runPtahNativeStdout(args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// triggerDefinition is how the server reports trigger t on table x.
func triggerDefinition(c *qt.C, target string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var definition string
	err = conn.QueryRowContext(c.Context(),
		`SELECT pg_get_triggerdef(oid) FROM pg_trigger WHERE tgname = 't' AND tgrelid = 'x'::regclass`,
	).Scan(&definition)
	c.Assert(err, qt.IsNil)
	return definition
}

// triggerChanges each start from one trigger and declare another spelling of
// its events, condition or transition tables. PostgreSQL 18 accepts both, and
// the SQL reader refused the event lists and TRUNCATE (stokaro/ptah#3674).
var triggerChanges = []struct {
	name, before, after, wantDefinition string
}{
	{
		name:           "an event joins the list",
		before:         `CREATE TRIGGER t BEFORE UPDATE OR INSERT ON x FOR EACH ROW EXECUTE FUNCTION f();`,
		after:          `CREATE TRIGGER t BEFORE UPDATE OR INSERT OR DELETE ON x FOR EACH ROW EXECUTE FUNCTION f();`,
		wantDefinition: `CREATE TRIGGER t BEFORE INSERT OR DELETE OR UPDATE ON public.x FOR EACH ROW EXECUTE FUNCTION f()`,
	},
	{
		name:           "update columns change",
		before:         `CREATE TRIGGER t BEFORE UPDATE OF b, a OR DELETE ON x FOR EACH ROW EXECUTE FUNCTION f();`,
		after:          `CREATE TRIGGER t BEFORE UPDATE OF "Total", B ON x FOR EACH ROW EXECUTE FUNCTION f();`,
		wantDefinition: `CREATE TRIGGER t BEFORE UPDATE OF "Total", b ON public.x FOR EACH ROW EXECUTE FUNCTION f()`,
	},
	{
		name:           "a statement trigger on truncate gains an event",
		before:         `CREATE TRIGGER t AFTER TRUNCATE ON x FOR STATEMENT EXECUTE FUNCTION f();`,
		after:          `CREATE TRIGGER t AFTER TRUNCATE OR DELETE ON x FOR EACH STATEMENT EXECUTE FUNCTION f();`,
		wantDefinition: `CREATE TRIGGER t AFTER DELETE OR TRUNCATE ON public.x FOR EACH STATEMENT EXECUTE FUNCTION f()`,
	},
	{
		name:           "a condition changes",
		before:         `CREATE TRIGGER t BEFORE UPDATE ON x FOR EACH ROW WHEN (NEW.a IS DISTINCT FROM OLD.a) EXECUTE FUNCTION f();`,
		after:          `CREATE TRIGGER t BEFORE UPDATE ON x FOR EACH ROW WHEN (NEW.a IN (1, 2) AND NEW.b > 0) EXECUTE FUNCTION f();`,
		wantDefinition: `CREATE TRIGGER t BEFORE UPDATE ON public.x FOR EACH ROW WHEN (((new.a = ANY (ARRAY[1, 2])) AND (new.b > 0))) EXECUTE FUNCTION f()`,
	},
	{
		name: "transition tables change",
		before: `CREATE TRIGGER t AFTER UPDATE ON x REFERENCING OLD TABLE old_rows NEW TABLE AS new_rows
  FOR EACH STATEMENT EXECUTE FUNCTION f();`,
		after:          `CREATE TRIGGER t AFTER UPDATE ON x REFERENCING NEW TABLE AS "After Rows" FOR EACH STATEMENT EXECUTE FUNCTION f();`,
		wantDefinition: `CREATE TRIGGER t AFTER UPDATE ON public.x REFERENCING NEW TABLE AS "After Rows" FOR EACH STATEMENT EXECUTE FUNCTION f()`,
	},
}

// TestSchemaApplyConvergesOnATriggerChangeLive applies each starting trigger
// to an empty database and asks again, then applies the changed one and asks
// again. Each second plan is empty, and the server reports the trigger the
// schema declared. So the apply created what it read, the comparison matched
// the server's spelling to the declaration, and a changed clause replaced the
// trigger rather than leaving the old one in place.
func TestSchemaApplyConvergesOnATriggerChangeLive(t *testing.T) {
	for _, test := range triggerChanges {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			before := writeTriggerSchema(c, test.before)
			after := writeTriggerSchema(c, test.after)

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", before, "--auto-approve")
			c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", before, "--dry-run"),
				qt.Contains, "Schema is synced")

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--auto-approve")
			c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--dry-run"),
				qt.Contains, "Schema is synced")
			c.Assert(triggerDefinition(c, target), qt.Equals, test.wantDefinition)
		})
	}
}

// TestSchemaInspectTriggerClausesRoundTripLive reads each changed trigger back
// as SQL and applies that SQL to a second empty database. The inspected
// definition, with the server's spelling of every clause, has to be a schema
// Ptah reads, and the trigger it creates has to be the one it was read from.
func TestSchemaInspectTriggerClausesRoundTripLive(t *testing.T) {
	for _, test := range triggerChanges {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source, _ := scratchReplayDatabase(c)
			copied, _ := scratchReplayDatabase(c)
			runPtahNative(c, "schema", "apply", "--db-url", source, "--schema-file", writeTriggerSchema(c, test.after), "--auto-approve")

			inspected, err := runPtahNativeStdout("schema", "inspect", "--db-url", source, "--format", "sql")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", inspected))
			path := filepath.Join(c.TempDir(), "inspected.sql")
			c.Assert(os.WriteFile(path, []byte(inspected), 0o600), qt.IsNil)

			runPtahNative(c, "schema", "apply", "--db-url", copied, "--schema-file", path, "--auto-approve")
			c.Assert(runPtahNative(c, "schema", "apply", "--db-url", copied, "--schema-file", path, "--dry-run"),
				qt.Contains, "Schema is synced")
			c.Assert(triggerDefinition(c, copied), qt.Equals, test.wantDefinition)
		})
	}
}

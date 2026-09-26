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

// writeHCLTriggerSchema writes a table and a trigger on it, whose timing block
// holds eventLines, to an HCL schema file of their own.
func writeHCLTriggerSchema(c *qt.C, eventLines string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.hcl")
	document := `schema "public" {}

table "x" {
  schema = schema.public
  column "id" { type = int }
  primary_key { columns = [column.id] }
}

trigger "t" {
  on = table.x
  before {
    ` + eventLines + `
  }
  for = ROW
  as  = "BEGIN RETURN NEW; END;"
}
`
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// hclTriggerDefinition is how the server reports trigger t on table x.
func hclTriggerDefinition(c *qt.C, target string) string {
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

// inspectHCL runs the native inspect in-process and returns the HCL it wrote
// to standard output, without the warnings it writes to standard error.
func inspectHCL(c *qt.C, target string) string {
	c.Helper()
	cmd := root.NewRootCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs([]string{"schema", "inspect", "--db-url", target})
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("stderr:\n%s", errOut.String()))
	return out.String()
}

// TestSchemaApplyCreatesEveryHCLTriggerEventLive applies an HCL trigger
// setting two events, then three, and reads each back from the server
// (stokaro/ptah#3692). Reading only the first event created a trigger on
// INSERT alone, and the plan after it was empty all the same, because the
// comparison read the same one event out of the document.
func TestSchemaApplyCreatesEveryHCLTriggerEventLive(t *testing.T) {
	c := qt.New(t)
	target, _ := scratchReplayDatabase(c)
	two := writeHCLTriggerSchema(c, "update = true\n    insert = true")
	three := writeHCLTriggerSchema(c, "update = true\n    insert = true\n    delete = true")

	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", two, "--auto-approve")
	c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", two, "--dry-run"),
		qt.Contains, "Schema is synced")
	c.Assert(hclTriggerDefinition(c, target), qt.Equals,
		"CREATE TRIGGER t BEFORE INSERT OR UPDATE ON public.x FOR EACH ROW EXECUTE FUNCTION ptah_trigger_public_x_t()")

	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", three, "--auto-approve")
	c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", three, "--dry-run"),
		qt.Contains, "Schema is synced")
	c.Assert(hclTriggerDefinition(c, target), qt.Equals,
		"CREATE TRIGGER t BEFORE INSERT OR DELETE OR UPDATE ON public.x FOR EACH ROW EXECUTE FUNCTION ptah_trigger_public_x_t()")
}

// TestSchemaInspectWritesEveryHCLTriggerEventLive inspects a trigger on two
// events as HCL and plans that document against a second, empty database.
// The trigger is written with both events, and the database the document
// creates holds the same trigger as the one it was read from.
func TestSchemaInspectWritesEveryHCLTriggerEventLive(t *testing.T) {
	c := qt.New(t)
	source, _ := scratchReplayDatabase(c)
	copied, _ := scratchReplayDatabase(c)
	runPtahNative(c, "schema", "apply", "--db-url", source,
		"--schema-file", writeHCLTriggerSchema(c, "insert = true\n    update = true"), "--auto-approve")

	inspected := inspectHCL(c, source)
	c.Assert(inspected, qt.Contains, "  before {\n    insert = true\n    update = true\n  }\n")
	path := filepath.Join(c.TempDir(), "inspected.hcl")
	c.Assert(os.WriteFile(path, []byte(inspected), 0o600), qt.IsNil)

	runPtahNative(c, "schema", "apply", "--db-url", copied, "--schema-file", path, "--auto-approve")
	c.Assert(runPtahNative(c, "schema", "apply", "--db-url", copied, "--schema-file", path, "--dry-run"),
		qt.Contains, "Schema is synced")
	c.Assert(hclTriggerDefinition(c, copied), qt.Equals, hclTriggerDefinition(c, source))
}

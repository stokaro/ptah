//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyPreservesAndChangesTriggerArgumentsLive starts with an
// argument-bearing trigger already in PostgreSQL. Ptah must not plan to drop
// it, and changing its argument must change the trigger's actual behavior.
func TestSchemaApplyPreservesAndChangesTriggerArgumentsLive(t *testing.T) {
	c := qt.New(t)
	target, conn := scratchReplayDatabase(c)
	const prefix = `CREATE TABLE x (id integer PRIMARY KEY, value text);
CREATE FUNCTION f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.value := TG_ARGV[0]; RETURN NEW; END $$;
`
	before := prefix + `CREATE TRIGGER t BEFORE UPDATE ON x FOR EACH ROW EXECUTE FUNCTION f('updated_at');`
	_, err := conn.ExecContext(c.Context(), before)
	c.Assert(err, qt.IsNil)
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(before), 0o600), qt.IsNil)
	c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", path, "--dry-run"),
		qt.Contains, "Schema is synced")

	after := prefix + `CREATE TRIGGER t BEFORE UPDATE ON x FOR EACH ROW EXECUTE FUNCTION f('created_at');`
	c.Assert(os.WriteFile(path, []byte(after), 0o600), qt.IsNil)
	plan := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", path, "--dry-run")
	c.Assert(plan, qt.Contains, "created_at")
	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", path, "--auto-approve")
	c.Assert(runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", path, "--dry-run"),
		qt.Contains, "Schema is synced")
	_, err = conn.ExecContext(c.Context(), `INSERT INTO x (id) VALUES (1); UPDATE x SET value = 'wrong' WHERE id = 1`)
	c.Assert(err, qt.IsNil)
	var value string
	c.Assert(conn.QueryRowContext(c.Context(), `SELECT value FROM x WHERE id = 1`).Scan(&value), qt.IsNil)
	c.Assert(value, qt.Equals, "created_at")
	c.Assert(triggerDefinition(c, target), qt.Contains, `f('created_at')`)

	inspected, err := runPtahNativeStdout("schema", "inspect", "--db-url", target, "--format", "sql")
	c.Assert(err, qt.IsNil)
	c.Assert(strings.ToLower(inspected), qt.Contains, "created_at")
}

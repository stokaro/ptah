//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// publicSchemaState reads "public" the way a role connecting later meets it:
// the owner, the grants sorted into one line, and the comment.
func publicSchemaState(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	var state string
	c.Assert(conn.QueryRowContext(c.Context(), `
		SELECT pg_get_userbyid(n.nspowner) || ' | ' ||
		       coalesce((
		           SELECT string_agg(g.line, ', ' ORDER BY g.line)
		           FROM (
		               SELECT CASE a.grantee WHEN 0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END
		                   || ' ' || a.privilege_type AS line
		               FROM aclexplode(coalesce(n.nspacl, acldefault('n', n.nspowner))) a
		           ) g
		       ), '') || ' | ' || coalesce(obj_description(n.oid, 'pg_namespace'), '')
		FROM pg_namespace n
		WHERE n.nspname = 'public'`).Scan(&state), qt.IsNil)
	return state
}

// TestMigrateValidateOnADevURLSelectingAnotherSchemaKeepsPublicE2E replays a
// directory on a dev database whose URL selects the schema "app". The replay's
// cleanup drops "public" with the rest of the realm and brings it back with
// the owner, grants and comment it had. Brought back bare, it was owned by the
// connecting role and PUBLIC lost USAGE on it (stokaro/ptah#3657).
func TestMigrateValidateOnADevURLSelectingAnotherSchemaKeepsPublicE2E(t *testing.T) {
	c := qt.New(t)
	dev, _ := scratchReplayDatabase(c)
	conn, err := dbschema.ConnectToDatabase(c.Context(), dev)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "CREATE SCHEMA app")
	c.Assert(err, qt.IsNil)
	before := publicSchemaState(c, conn)
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_t.sql"), []byte("CREATE TABLE t (id int);\n"), 0o600), qt.IsNil)
	out, err := runCompatVerb("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

	out, err = runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", dev+"&search_path=app")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(publicSchemaState(c, conn), qt.Equals, before)
}

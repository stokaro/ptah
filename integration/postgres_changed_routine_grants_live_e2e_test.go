//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// The generated up and down files keep the same access list on g when its
// integer argument becomes bigint and returns to integer. One grant belongs
// to an ordinary role and one to pg_monitor. The ordinary role's grant is
// tested both declared and outside the schema's management.
// Another function's ACL stays untouched, and a PUBLIC revoke stays revoked.
func TestPostgresChangedRoutineKeepsGrantsLiveE2E(t *testing.T) {
	const managed = "CREATE ROLE ROLE_NAME;\nGRANT EXECUTE ON FUNCTION g(bigint) TO ROLE_NAME WITH GRANT OPTION;\n"
	for _, public := range []struct{ name, revoke, grants string }{
		{name: "unmanaged role and PUBLIC may execute"},
		{name: "unmanaged role and PUBLIC may not execute", revoke: "REVOKE EXECUTE ON FUNCTION g(integer) FROM PUBLIC;\n"},
		{name: "managed role and PUBLIC may execute", grants: managed},
		{name: "managed role and PUBLIC may not execute", grants: managed, revoke: "REVOKE EXECUTE ON FUNCTION g(integer) FROM PUBLIC;\n"},
	} {
		t.Run(public.name, func(t *testing.T) {
			c := qt.New(t)
			role := changedRoutineGrantRole(c)
			database := postgresUnloggedDatabase(c)
			dir := c.TempDir()
			initial := "CREATE FUNCTION g(a integer) RETURNS text LANGUAGE sql AS $$SELECT a::text$$;\n" +
				"CREATE FUNCTION untouched(a text) RETURNS text LANGUAGE sql AS $$SELECT a$$;\n" + public.revoke +
				"GRANT EXECUTE ON FUNCTION g(integer) TO " + role + " WITH GRANT OPTION;\n" +
				"GRANT EXECUTE ON FUNCTION g(integer) TO pg_monitor;\n" +
				"GRANT EXECUTE ON FUNCTION untouched(text) TO pg_read_all_settings;\n"
			writeRoutineMigration(c, dir, "1700000000_init.up.sql", initial)
			writeRoutineMigration(c, dir, "1700000000_init.down.sql", "DROP FUNCTION g(integer); DROP FUNCTION untouched(text);\n")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(
				"CREATE FUNCTION g(a bigint) RETURNS text LANGUAGE sql AS $$SELECT a::text$$;\n"+
					"CREATE FUNCTION untouched(a text) RETURNS text LANGUAGE sql AS $$SELECT a$$;\n"+
					strings.ReplaceAll(public.grants, "ROLE_NAME", role)), 0o600), qt.IsNil)
			initialized, err := runPtahNativeWithError("migrations", "up", "--db-url", database.url, "--migrations-dir", dir)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", initialized))
			held := changedRoutineACL(c, database, "g(integer)")
			other := changedRoutineACL(c, database, "untouched(text)")
			c.Assert(held, qt.Contains, "pg_monitor=X/")
			c.Assert(held, qt.Contains, role+"=X*/")
			generated, err := runPtahNativeWithError("migrations", "generate", "--db-url", database.url,
				"--migrations-dir", dir, "--schema-file", schema, "--name", "signature")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", generated))
			c.Logf("up:\n%s\ndown:\n%s", routineMigrationNamed(c, dir, "_signature.up.sql"), routineMigrationNamed(c, dir, "_signature.down.sql"))

			applied, upErr := runPtahNativeWithError("migrations", "up", "--db-url", database.url, "--migrations-dir", dir, "--allow-destructive")
			c.Assert(upErr, qt.IsNil, qt.Commentf("%s", applied))
			changed := changedRoutineACL(c, database, "g(bigint)")
			c.Assert(changedRoutineACL(c, database, "untouched(text)"), qt.Equals, other)
			rolledBack, downErr := runPtahNativeWithError("migrations", "down", "--db-url", database.url,
				"--migrations-dir", dir, "--limit", "1", "--confirm")

			c.Assert(downErr, qt.IsNil, qt.Commentf("%s", rolledBack))
			c.Check(changed, qt.Equals, held)
			c.Check(changedRoutineACL(c, database, "g(integer)"), qt.Equals, held)
			c.Assert(changedRoutineACL(c, database, "untouched(text)"), qt.Equals, other)
		})
	}
}

// The role is cluster-wide and outlives the scratch database cleanup, so its
// cleanup is registered first and runs after that database is dropped.
func changedRoutineGrantRole(c *qt.C) string {
	c.Helper()
	admin := openReadScopeDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL))
	role := fmt.Sprintf("ptah_changed_routine_%d", time.Now().UnixNano())
	_, err := admin.ExecContext(c.Context(), "CREATE ROLE "+role)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP ROLE "+role)
		c.Check(err, qt.IsNil)
	})
	return role
}

// Read proacl itself, sorted only to disregard the order of its entries.
func changedRoutineACL(c *qt.C, database devDialectDatabase, signature string) string {
	c.Helper()
	var acl string
	c.Assert(database.conn.QueryRowContext(c.Context(), `
		SELECT COALESCE(string_agg(entry::text, ',' ORDER BY entry::text), '')
		FROM pg_proc p CROSS JOIN LATERAL unnest(p.proacl) entry
		WHERE p.oid = $1::regprocedure`, signature).Scan(&acl), qt.IsNil)
	return acl
}

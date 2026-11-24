//go:build integration

package integration_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// What `ptah db read` writes for a privilege named in more than one word is
// read back as the schema it describes, and a plan against the database it
// came from is empty. Applied without some of the grants it wrote, the file
// revokes exactly those, and applied whole again it grants them back: the
// server answers each statement, and its catalog is read after each apply. A
// last plan of the whole file is empty.
// MySQL and MariaDB name SHOW VIEW on a table, and LOCK TABLES and CREATE
// TEMPORARY TABLES on a database, which they write as `db`.*.
func TestDBReadMultiWordPrivilegesReadBackMySQLFamilyE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, engine.admin, "CREATE TABLE t1 (id INT PRIMARY KEY);\n", []string{
				"GRANT SELECT, SHOW VIEW ON `%[1]s`.t1 TO %[2]s",
				"GRANT LOCK TABLES, CREATE TEMPORARY TABLES ON `%[1]s`.* TO %[2]s",
			})
			held := []string{"CREATE TEMPORARY TABLES ON *", "LOCK TABLES ON *", "SELECT ON t1", "SHOW VIEW ON t1"}

			trip := roundTripGrants(c, run.url, `(?m)^GRANT (LOCK TABLES|SHOW VIEW) ON .*\n`, func() []string {
				return grantRows(c, run.scratch.admin, `
					SELECT CONCAT(PRIVILEGE_TYPE, ' ON *') FROM information_schema.SCHEMA_PRIVILEGES
					WHERE GRANTEE LIKE ? AND TABLE_SCHEMA = ?
					UNION ALL
					SELECT CONCAT(PRIVILEGE_TYPE, ' ON ', TABLE_NAME) FROM information_schema.TABLE_PRIVILEGES
					WHERE GRANTEE LIKE ? AND TABLE_SCHEMA = ?
					ORDER BY 1`, "'"+run.role+"'%", run.database, "'"+run.role+"'%", run.database)
			})

			c.Assert(trip.before, qt.DeepEquals, held)
			c.Assert(trip.read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.read.Stdout, trip.read.Stderr))
			c.Assert(trip.read.Stdout, qt.Matches, `(?s).*GRANT SHOW VIEW ON `+"`"+run.database+"`"+`\.`+"`t1`"+` .*`)
			c.Assert(trip.read.Stdout, qt.Matches, `(?s).*GRANT LOCK TABLES ON `+"`"+run.database+"`"+`\.\* .*`)
			c.Assert(trip.read.Stdout, qt.Matches, `(?s).*GRANT CREATE TEMPORARY TABLES ON `+"`"+run.database+"`"+`\.\* .*`)
			c.Assert(trip.synced.ExitCode, qt.Equals, 0, qt.Commentf("read:\n%s\nstdout:\n%s\nstderr:\n%s", trip.read.Stdout, trip.synced.Stdout, trip.synced.Stderr))
			c.Assert(trip.synced.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(trip.revoked.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.revoked.Stdout, trip.revoked.Stderr))
			c.Assert(trip.revoked.Stdout, qt.Matches, `(?s).*REVOKE LOCK TABLES ON .*`)
			c.Assert(trip.revoked.Stdout, qt.Matches, `(?s).*REVOKE SHOW VIEW ON .*`)
			c.Assert(trip.revoked.Stdout, qt.Not(qt.Matches), `(?s).*(REVOKE|GRANT) (CREATE TEMPORARY TABLES|SELECT) .*`)
			c.Assert(trip.afterRevoke, qt.DeepEquals, []string{"CREATE TEMPORARY TABLES ON *", "SELECT ON t1"})
			c.Assert(trip.granted.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.granted.Stdout, trip.granted.Stderr))
			c.Assert(trip.granted.Stdout, qt.Matches, `(?s).*GRANT LOCK TABLES ON .*`)
			c.Assert(trip.granted.Stdout, qt.Matches, `(?s).*GRANT SHOW VIEW ON .*`)
			c.Assert(trip.granted.Stdout, qt.Not(qt.Matches), `(?s).*(REVOKE|GRANT) (CREATE TEMPORARY TABLES|SELECT) .*`)
			c.Assert(trip.afterGrant, qt.DeepEquals, held)
			c.Assert(trip.resynced.Stdout, qt.Contains, "Schema is synced, no changes to be made.", qt.Commentf("stderr:\n%s", trip.resynced.Stderr))
		})
	}
}

// SQL Server names VIEW DEFINITION and TAKE OWNERSHIP on an object, and a
// permission on a schema as ON SCHEMA::[name].
func TestDBReadMultiWordPermissionsReadBackSQLServerE2E(t *testing.T) {
	c := qt.New(t)
	server := newSQLServerRevokeAllServer(c,
		"GRANT VIEW DEFINITION, TAKE OWNERSHIP ON dbo.t1 TO [{role}]; "+
			"GRANT SELECT, VIEW DEFINITION ON SCHEMA::dbo TO [{role}]")
	held := []string{"SELECT ON SCHEMA", "TAKE OWNERSHIP ON t1", "VIEW DEFINITION ON SCHEMA", "VIEW DEFINITION ON t1"}

	trip := roundTripGrants(c, server.url, `(?m)^GRANT (TAKE OWNERSHIP ON |VIEW DEFINITION ON SCHEMA::).*\n`, func() []string {
		return grantRows(c, server.catalog, `
			SELECT CONCAT(p.permission_name, ' ON ', CASE p.class WHEN 3 THEN 'SCHEMA' ELSE 't1' END)
			FROM sys.database_permissions p
			JOIN sys.database_principals d ON d.principal_id = p.grantee_principal_id
			WHERE d.name = @p1 AND p.state IN ('G', 'W')
				AND ((p.class = 1 AND p.major_id = OBJECT_ID('dbo.t1')) OR (p.class = 3 AND p.major_id = SCHEMA_ID('dbo')))
			ORDER BY 1`, server.role)
	})

	c.Assert(trip.before, qt.DeepEquals, held)
	c.Assert(trip.read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.read.Stdout, trip.read.Stderr))
	c.Assert(trip.read.Stdout, qt.Matches, `(?s).*GRANT VIEW DEFINITION ON \[dbo\]\.\[t1\] .*`)
	c.Assert(trip.read.Stdout, qt.Matches, `(?s).*GRANT TAKE OWNERSHIP ON \[dbo\]\.\[t1\] .*`)
	c.Assert(trip.read.Stdout, qt.Matches, `(?s).*GRANT VIEW DEFINITION ON SCHEMA::\[dbo\] .*`)
	c.Assert(trip.synced.ExitCode, qt.Equals, 0, qt.Commentf("read:\n%s\nstdout:\n%s\nstderr:\n%s", trip.read.Stdout, trip.synced.Stdout, trip.synced.Stderr))
	c.Assert(trip.synced.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
	c.Assert(trip.revoked.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.revoked.Stdout, trip.revoked.Stderr))
	c.Assert(trip.revoked.Stdout, qt.Matches, `(?s).*REVOKE TAKE OWNERSHIP ON \[dbo\]\.\[t1\] .*`)
	c.Assert(trip.revoked.Stdout, qt.Matches, `(?s).*REVOKE VIEW DEFINITION ON SCHEMA::\[dbo\] .*`)
	c.Assert(trip.revoked.Stdout, qt.Not(qt.Matches), `(?s).*(REVOKE|GRANT) (SELECT|VIEW DEFINITION ON \[dbo\]) .*`)
	c.Assert(trip.afterRevoke, qt.DeepEquals, []string{"SELECT ON SCHEMA", "VIEW DEFINITION ON t1"})
	c.Assert(trip.granted.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.granted.Stdout, trip.granted.Stderr))
	c.Assert(trip.granted.Stdout, qt.Matches, `(?s).*GRANT TAKE OWNERSHIP ON \[dbo\]\.\[t1\] .*`)
	c.Assert(trip.granted.Stdout, qt.Matches, `(?s).*GRANT VIEW DEFINITION ON SCHEMA::\[dbo\] .*`)
	c.Assert(trip.afterGrant, qt.DeepEquals, held)
	c.Assert(trip.resynced.Stdout, qt.Contains, "Schema is synced, no changes to be made.", qt.Commentf("stderr:\n%s", trip.resynced.Stderr))
}

// Oracle names ON COMMIT REFRESH and QUERY REWRITE on a table.
func TestDBReadMultiWordPrivilegesReadBackOracleE2E(t *testing.T) {
	c := qt.New(t)
	server, _ := newOracleRevokeAllServer(c, "GRANT SELECT, ON COMMIT REFRESH, QUERY REWRITE ON t1 TO {role}")

	trip := roundTripGrants(c, server.url, `(?m)^GRANT (ON COMMIT REFRESH|QUERY REWRITE) ON .*\n`, func() []string {
		return server.privileges(c)
	})

	c.Assert(trip.before, qt.DeepEquals, []string{"ON COMMIT REFRESH", "QUERY REWRITE", "SELECT"})
	c.Assert(trip.read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.read.Stdout, trip.read.Stderr))
	c.Assert(trip.read.Stdout, qt.Matches, `(?s).*GRANT ON COMMIT REFRESH ON .*`)
	c.Assert(trip.read.Stdout, qt.Matches, `(?s).*GRANT QUERY REWRITE ON .*`)
	c.Assert(trip.synced.ExitCode, qt.Equals, 0, qt.Commentf("read:\n%s\nstdout:\n%s\nstderr:\n%s", trip.read.Stdout, trip.synced.Stdout, trip.synced.Stderr))
	c.Assert(trip.synced.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
	c.Assert(trip.revoked.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.revoked.Stdout, trip.revoked.Stderr))
	c.Assert(trip.revoked.Stdout, qt.Matches, `(?s).*REVOKE ON COMMIT REFRESH ON .*`)
	c.Assert(trip.revoked.Stdout, qt.Matches, `(?s).*REVOKE QUERY REWRITE ON .*`)
	c.Assert(trip.revoked.Stdout, qt.Not(qt.Matches), `(?s).*(REVOKE|GRANT) SELECT .*`)
	c.Assert(trip.afterRevoke, qt.DeepEquals, []string{"SELECT"})
	c.Assert(trip.granted.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.granted.Stdout, trip.granted.Stderr))
	c.Assert(trip.granted.Stdout, qt.Matches, `(?s).*GRANT ON COMMIT REFRESH ON .*`)
	c.Assert(trip.granted.Stdout, qt.Matches, `(?s).*GRANT QUERY REWRITE ON .*`)
	c.Assert(trip.afterGrant, qt.DeepEquals, []string{"ON COMMIT REFRESH", "QUERY REWRITE", "SELECT"})
	c.Assert(trip.resynced.Stdout, qt.Contains, "Schema is synced, no changes to be made.", qt.Commentf("stderr:\n%s", trip.resynced.Stderr))
}

// grantRoundTrip is what one round trip through `ptah db read` saw: the
// commands it ran, and what the server's catalog held before it began and
// after each apply.
type grantRoundTrip struct {
	read, synced, revoked, granted  clirun.Result
	resynced                        clirun.Result
	before, afterRevoke, afterGrant []string
}

// roundTripGrants reads the database at url with `ptah db read` and plans the
// output as the desired schema. It then applies the output less every line
// dropped matches, and the whole output again, and asks held after each. It
// plans the whole output once more at the end.
func roundTripGrants(c *qt.C, url, dropped string, held func() []string) grantRoundTrip {
	c.Helper()
	trip := grantRoundTrip{before: held(), read: runPtahIn(c, "db", "read", "--db-url", url)}
	trip.synced = applyReadBack(c, url, trip.read.Stdout, "--dry-run")
	trip.revoked = applyReadBack(c, url, regexp.MustCompile(dropped).ReplaceAllString(trip.read.Stdout, ""), "--auto-approve")
	trip.afterRevoke = held()
	trip.granted = applyReadBack(c, url, trip.read.Stdout, "--auto-approve")
	trip.afterGrant = held()
	trip.resynced = applyReadBack(c, url, trip.read.Stdout, "--dry-run")
	return trip
}

// applyReadBack runs `ptah schema apply` against url with schema as the
// desired schema and the extra argument.
func applyReadBack(c *qt.C, url, schema, mode string) clirun.Result {
	c.Helper()
	file := writeDeclaredSchemaFile(c, "read.sql", schema)
	return runPtahIn(c, "schema", "apply", "--db-url", url, "--schema-file", file, mode)
}

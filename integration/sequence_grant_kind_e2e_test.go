//go:build integration

package integration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// postgresSequencePrivileges lists, in order, the privileges role holds on the
// sequence s1, as its ACL reports them.
const postgresSequencePrivileges = `
	SELECT a.privilege_type FROM pg_class c
	CROSS JOIN LATERAL aclexplode(c.relacl) a
	JOIN pg_roles r ON r.oid = a.grantee
	WHERE c.relname = 's1' AND c.relkind = 'S' AND r.rolname = $1
	ORDER BY 1`

// oracleSequencePrivileges lists, in order, the privileges role holds on the
// sequence S1 of the schema the second argument names.
const oracleSequencePrivileges = `
	SELECT privilege FROM all_tab_privs
	WHERE grantee = :1 AND table_schema = :2 AND table_name = 'S1' AND type = 'SEQUENCE'
	ORDER BY privilege`

// What `ptah db read` writes for a grant on a sequence is read back as a grant
// on the sequence, as the read reports it. A plan against the database it came
// from is empty, an apply of the file without the grant revokes it, an apply of
// the whole file grants it back, and a last plan is empty.
func TestDBReadSequenceGrantsReadBackPostgresE2E(t *testing.T) {
	c := qt.New(t)
	server := newPostgresRevokeAllServer(c, "CREATE SEQUENCE s1")
	execRevokeAllSetup(c, server.catalog, server.role, "", "GRANT SELECT, UPDATE ON s1 TO {role}")
	held := []string{"SELECT", "UPDATE"}

	trip := roundTripGrants(c, server.url, `(?m)^GRANT [A-Z, ]+ ON SEQUENCE .*\n`, func() []string {
		return grantRows(c, server.catalog, postgresSequencePrivileges, server.role)
	})

	c.Assert(trip.before, qt.DeepEquals, held)
	c.Assert(trip.read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.read.Stdout, trip.read.Stderr))
	c.Assert(trip.synced.Stdout, qt.Contains, "Schema is synced, no changes to be made.", qt.Commentf("read:\n%s\nstderr:\n%s", trip.read.Stdout, trip.synced.Stderr))
	c.Assert(trip.revoked.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.revoked.Stdout, trip.revoked.Stderr))
	c.Assert(trip.afterRevoke, qt.HasLen, 0)
	c.Assert(trip.granted.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.granted.Stdout, trip.granted.Stderr))
	c.Assert(trip.afterGrant, qt.DeepEquals, held)
	c.Assert(trip.resynced.Stdout, qt.Contains, "Schema is synced, no changes to be made.", qt.Commentf("stderr:\n%s", trip.resynced.Stderr))
}

// Oracle's GRANT names no object kind, so `ptah db read` writes a grant on a
// sequence as it writes one on a table. Read as a table grant, the file does
// not match the sequence grant the read reports: the plan grants the
// privileges and revokes them in the same run, and the role holds nothing on
// the sequence after the apply.
func TestDBReadSequenceGrantsReadBackOracleE2E(t *testing.T) {
	c := qt.New(t)
	server, account := newOracleSequenceGrantServer(c, "GRANT KEEP SEQUENCE, SELECT ON s1 TO {role}")
	held := []string{"KEEP SEQUENCE", "SELECT"}

	trip := roundTripGrants(c, server.url, `(?m)^GRANT [A-Z, ]+ ON [^ ]*S1 .*\n`, func() []string {
		return grantRows(c, server.catalog, oracleSequencePrivileges, server.role, account)
	})

	c.Assert(trip.before, qt.DeepEquals, held)
	c.Assert(trip.read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.read.Stdout, trip.read.Stderr))
	c.Assert(trip.read.Stdout, qt.Matches, `(?s).*GRANT KEEP SEQUENCE.* ON [^ ]*S1 .*`)
	c.Assert(trip.synced.Stdout, qt.Contains, "Schema is synced, no changes to be made.", qt.Commentf("read:\n%s\nstderr:\n%s", trip.read.Stdout, trip.synced.Stderr))
	c.Assert(trip.revoked.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.revoked.Stdout, trip.revoked.Stderr))
	c.Assert(trip.afterRevoke, qt.HasLen, 0)
	c.Assert(trip.granted.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", trip.granted.Stdout, trip.granted.Stderr))
	c.Assert(trip.granted.Stdout, qt.Not(qt.Matches), `(?s).*REVOKE .*`)
	c.Assert(trip.afterGrant, qt.DeepEquals, held)
	c.Assert(trip.resynced.Stdout, qt.Contains, "Schema is synced, no changes to be made.", qt.Commentf("stderr:\n%s", trip.resynced.Stderr))
}

// A grant on a sequence written by hand without SEQUENCE is a grant on the
// sequence: the apply grants it, the role holds it afterwards, and the next
// plan is empty.
func TestSchemaApplyPostgresGrantsOnASequenceByNameE2E(t *testing.T) {
	c := qt.New(t)
	server := newPostgresRevokeAllServer(c, "CREATE SEQUENCE s1")
	declared := strings.ReplaceAll("CREATE TABLE t1 (id int PRIMARY KEY, name text);\nCREATE SEQUENCE s1;\n"+
		"GRANT SELECT, UPDATE ON s1 TO {role};\n", "{role}", server.role)

	applied := applyReadBack(c, server.url, declared, "--auto-approve")
	held := grantRows(c, server.catalog, postgresSequencePrivileges, server.role)
	replanned := applyReadBack(c, server.url, declared, "--dry-run")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(held, qt.DeepEquals, []string{"SELECT", "UPDATE"})
	c.Assert(replanned.Stdout, qt.Contains, "Schema is synced, no changes to be made.", qt.Commentf("stderr:\n%s", replanned.Stderr))
}

// The same grant on Oracle, whose GRANT has no SEQUENCE to write.
func TestSchemaApplyOracleGrantsOnASequenceByNameE2E(t *testing.T) {
	c := qt.New(t)
	server, account := newOracleSequenceGrantServer(c)
	declared := strings.ReplaceAll("CREATE TABLE t1 (id NUMBER(10) PRIMARY KEY, name VARCHAR2(50));\nCREATE SEQUENCE s1;\n"+
		"GRANT SELECT ON t1 TO {role};\nGRANT KEEP SEQUENCE, SELECT ON s1 TO {role};\n", "{role}", server.role)

	applied := applyReadBack(c, server.url, declared, "--auto-approve")
	held := grantRows(c, server.catalog, oracleSequencePrivileges, server.role, account)
	replanned := applyReadBack(c, server.url, declared, "--dry-run")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(held, qt.DeepEquals, []string{"KEEP SEQUENCE", "SELECT"})
	c.Assert(replanned.Stdout, qt.Contains, "Schema is synced, no changes to be made.", qt.Commentf("stdout:\n%s\nstderr:\n%s", replanned.Stdout, replanned.Stderr))
}

// newOracleSequenceGrantServer is [newOracleRevokeAllServer] with the role
// holding SELECT on t1 and the account owning the sequence s1, then runs held.
// The account is given CREATE SEQUENCE for it.
func newOracleSequenceGrantServer(c *qt.C, held ...string) (revokeAllServer, string) {
	c.Helper()
	server, account := newOracleRevokeAllServer(c, "GRANT SELECT ON t1 TO {role}")
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.OracleAdmin))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(admin) })
	execOracle(c.Context(), c, admin, "GRANT CREATE SEQUENCE TO "+account)
	execRevokeAllSetup(c, server.catalog, server.role, "", append([]string{"CREATE SEQUENCE s1"}, held...)...)
	return server, account
}

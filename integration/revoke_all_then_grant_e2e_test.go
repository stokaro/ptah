//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"net/url"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// newPostgresRevokeAllServer creates a database holding t1, the role, and a
// user the role is granted to, then runs held. The database, the user and the
// role are dropped when the test ends.
func newPostgresRevokeAllServer(c *qt.C, held string) revokeAllServer {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	role := revokeAllStamp()
	user := role + "_u"
	c.Cleanup(func() {
		for _, name := range []string{user, role} {
			_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(name))
			c.Check(dropErr, qt.IsNil, qt.Commentf("drop role %s", name))
		}
	})
	execRevokeAllSetup(c, admin, role, user,
		"CREATE ROLE {role}", "CREATE ROLE {user} LOGIN PASSWORD 'ptah_member_pw'", "GRANT {role} TO {user}")
	createE2EDatabase(c, c.Context(), admin, role)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, role) })
	databaseURL := replaceDatabaseName(c, adminURL, role)
	driverURL := postgresFamilyDriverURL(c, databaseURL)
	db, err := sql.Open("pgx", driverURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	execRevokeAllSetup(c, db, role, user,
		"CREATE TABLE t1 (id int PRIMARY KEY, name text)",
		"INSERT INTO t1 VALUES (1, 'a')",
		held)
	return revokeAllServer{
		url:     databaseURL,
		role:    role,
		catalog: db,
		heldQuery: "SELECT privilege_type || ' ' || is_grantable FROM information_schema.table_privileges " +
			"WHERE grantee = $1 AND table_schema = 'public' AND table_name = 't1' ORDER BY privilege_type",
		heldArgs: []any{role},
		member:   openRevokeAllMember(c, "pgx", driverURL, url.UserPassword(user, "ptah_member_pw")),
	}
}

// publicPrivileges is what PUBLIC holds on t1, one privilege and whether it is
// grantable per row, in order.
func (s revokeAllServer) publicPrivileges(c *qt.C) []string {
	c.Helper()
	public := s
	public.heldArgs = []any{"PUBLIC"}
	return public.privileges(c)
}

// A REVOKE ALL followed by a GRANT SELECT leaves the role SELECT and nothing
// else, without the grant option unless the grant asks for it. Without the
// revoke deciding the option, a role the schema does not create keeps it: the
// plan revokes the other privileges and leaves SELECT grantable, a member
// grants SELECT to PUBLIC, and the next plan reads as synced. Measured on
// PostgreSQL 18. A member without the option is warned rather than refused,
// so what PUBLIC holds afterwards is what says whether the grant went through.
func TestSchemaApplyPostgresRevokeAllThenGrantE2E(t *testing.T) {
	const table = "CREATE TABLE t1 (id int PRIMARY KEY, name text);\n"
	tests := []struct {
		name       string
		declared   string
		want       []string
		wantPublic []string
	}{
		{
			name:     "a role the schema does not create",
			declared: table + "REVOKE ALL ON t1 FROM {role};\nGRANT SELECT ON t1 TO {role};\n",
			want:     []string{"SELECT NO"},
		},
		{
			name:     "a role the schema creates",
			declared: table + "CREATE ROLE {role};\nREVOKE ALL ON t1 FROM {role};\nGRANT SELECT ON t1 TO {role};\n",
			want:     []string{"SELECT NO"},
		},
		{
			name:       "a grant that asks for the option",
			declared:   table + "REVOKE ALL ON t1 FROM {role};\nGRANT SELECT ON t1 TO {role} WITH GRANT OPTION;\n",
			want:       []string{"SELECT YES"},
			wantPublic: []string{"SELECT NO"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := newPostgresRevokeAllServer(c, "GRANT ALL ON t1 TO {role} WITH GRANT OPTION")

			server.converges(c, test.declared, test.want,
				[]string{"SELECT id FROM t1", "GRANT SELECT ON t1 TO PUBLIC"},
				[]string{"INSERT INTO t1 VALUES (2, 'b')", "TRUNCATE t1"},
				`(?s).*permission denied for table t1.*`)
			c.Assert(server.publicPrivileges(c), qt.DeepEquals, test.wantPublic)
		})
	}
}

// A REVOKE ALL followed by a GRANT SELECT leaves the role SELECT without the
// grant option. Without the revoke deciding the option, the plan for a role the
// schema does not create revokes the other permissions and leaves SELECT
// grantable, a member grants it onward, and the next plan reads as synced.
// Measured on SQL Server 2025.
func TestSchemaApplySQLServerRevokeAllThenGrantE2E(t *testing.T) {
	c := qt.New(t)
	server := newSQLServerRevokeAllServer(c, "GRANT SELECT, INSERT, ALTER ON dbo.t1 TO [{role}] WITH GRANT OPTION")

	server.converges(c,
		"CREATE TABLE t1 (id INT PRIMARY KEY, name NVARCHAR(50));\nREVOKE ALL ON t1 FROM [{role}];\nGRANT SELECT ON t1 TO [{role}];\n",
		[]string{"SELECT G"},
		[]string{"SELECT id FROM dbo.t1"},
		[]string{"INSERT INTO dbo.t1 VALUES (2, 'b')", "GRANT SELECT ON dbo.t1 TO public AS [{role}]"},
		`(?s).*(permission was denied|do not have permission|Grantor does not have GRANT permission).*`)
}

// A REVOKE ALL followed by a GRANT SELECT leaves the role SELECT without the
// grant option. ClickHouse refuses a grant to a role the schema does not
// declare, so the role here is one the schema creates. Measured on ClickHouse
// 26.10.
func TestSchemaApplyClickHouseRevokeAllThenGrantE2E(t *testing.T) {
	c := qt.New(t)
	server := newClickHouseRevokeAllServer(c, "GRANT ALL ON {role}.t1 TO {role} WITH GRANT OPTION")

	server.converges(c,
		"CREATE TABLE t1 (id UInt32, name String) ENGINE = MergeTree ORDER BY id;\n"+
			"CREATE ROLE {role};\nREVOKE ALL ON {role}.t1 FROM {role};\nGRANT SELECT ON {role}.t1 TO {role};\n",
		[]string{"SELECT 0"},
		[]string{"SELECT id FROM t1"},
		[]string{"INSERT INTO t1 VALUES (2, 'b')", "GRANT SELECT ON t1 TO {role}"},
		`(?s).*code: 497, .*Not enough privileges.*`)
}

// A REVOKE ALL followed by a GRANT SELECT leaves the role SELECT without the
// grant option. Without the revoke deciding the option, the plan for a role the
// schema does not create revokes the other privileges and leaves SELECT
// grantable. Measured on CockroachDB 26.4.1. A role holding GRANT ALL is not
// the case: the catalog reports it as one ALL row, and the revoke takes the
// option with it.
func TestSchemaApplyCockroachDBRevokeAllThenGrantE2E(t *testing.T) {
	c := qt.New(t)
	server := newCockroachRevokeAllServer(c, "GRANT SELECT, INSERT, DROP ON t1 TO {role} WITH GRANT OPTION")

	server.converges(c,
		"CREATE TABLE t1 (id INT PRIMARY KEY, name STRING);\nREVOKE ALL ON t1 FROM {role};\nGRANT SELECT ON t1 TO {role};\n",
		[]string{"SELECT false"},
		[]string{"SELECT id FROM t1"},
		[]string{"INSERT INTO t1 VALUES (2, 'b')", "GRANT SELECT ON t1 TO {role}"},
		`(?s).*(does not have .*privilege|missing WITH GRANT OPTION).*`)
}

// A REVOKE ALL followed by a GRANT SELECT leaves the role SELECT on t1 without
// the grant option, unless the grant asks for it. Without the revoke deciding
// the option, the plan for a role the schema does not create revokes the other
// privileges and leaves SELECT grantable, a member grants it onward, and the
// next plan reads as synced. Measured on MySQL 8.4.13 and MariaDB 11.8.9.
func TestSchemaApplyMySQLFamilyRevokeAllThenGrantE2E(t *testing.T) {
	held := []string{
		"GRANT ALL ON `%[1]s`.t1 TO %[2]s WITH GRANT OPTION",
		"GRANT SELECT ON `%[1]s`.t2 TO %[2]s",
	}
	tests := []struct {
		name     string
		declared string
		want     []string
		allowed  []string
		refused  []string
	}{
		{
			name:     "a grant without the option",
			declared: revokeAllSchema + "REVOKE ALL ON t1 FROM %[1]s;\nGRANT SELECT ON t1 TO %[1]s;\n",
			want:     []string{"table t1 SELECT NO", "table t2 SELECT NO"},
			allowed:  []string{"SELECT id FROM t1"},
			refused:  append([]string{"GRANT SELECT ON t1 TO CURRENT_USER"}, memberOfNothingOnT1[1:]...),
		},
		{
			name:     "a grant that asks for the option",
			declared: revokeAllSchema + "REVOKE ALL ON t1 FROM %[1]s;\nGRANT SELECT ON t1 TO %[1]s WITH GRANT OPTION;\n",
			want:     []string{"table t1 SELECT YES", "table t2 SELECT NO"},
			allowed:  []string{"SELECT id FROM t1", "GRANT SELECT ON t1 TO CURRENT_USER"},
			refused:  memberOfNothingOnT1[1:],
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, revokeAllSchema, held)

				applied := run.apply(c, test.declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(run.privileges(c), qt.DeepEquals, test.want)
				member := run.member(c)
				for _, statement := range test.allowed {
					_, err := member.ExecContext(c.Context(), statement)
					c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
				}
				for _, statement := range test.refused {
					_, err := member.ExecContext(c.Context(), statement)
					c.Assert(err, qt.ErrorMatches, `(?s).*command denied to user.*`, qt.Commentf("%s", statement))
				}

				again := run.apply(c, test.declared, "--dry-run")

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

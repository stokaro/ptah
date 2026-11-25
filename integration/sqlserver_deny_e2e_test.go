//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// A SQL Server DENY a schema file declares is applied, read back from
// sys.database_permissions with state 'D', and settles: the next plan is
// empty, and a user that holds the role is refused what the DENY withholds
// whatever grant on the schema gives it.
//
// A DENY on the whole table takes the column grants of its permission, and the
// server keeps a column grant written after one, so a schema file that keeps a
// column grant declares it after the DENY, and the plan grants it again after
// the DENY. A DENY of a permission the role holds
// WITH GRANT OPTION is Msg 4611 without CASCADE. A DENY the database holds and
// the schema leaves out stays: a removal planned from a document that cannot
// state a DENY would give the role what an administrator denied it.
func TestSchemaApplySQLServerDenyE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		held     []string
		declared string
		want     []string
		reads    string
		denied   string
		refusal  string
	}{
		{
			name:     "a deny on a column under a grant on the schema",
			declared: "GRANT SELECT ON SCHEMA::dbo TO cp_reader;\nDENY SELECT (secret) ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT DENY secret"},
			reads:    "SELECT id, name FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
		},
		{
			name:     "a deny on a table under a grant on the schema",
			declared: "GRANT SELECT, UPDATE ON SCHEMA::dbo TO cp_reader;\nDENY UPDATE ON dbo.t TO cp_reader;\n",
			want:     []string{"UPDATE DENY"},
			reads:    "SELECT id, name, secret FROM dbo.t",
			denied:   "UPDATE dbo.t SET name = N'b'",
			refusal:  `(?s).*The UPDATE permission was denied on the object 't'.*`,
		},
		{
			name:     "a deny on a table and a grant on its column",
			declared: "DENY SELECT ON dbo.t TO cp_reader;\nGRANT SELECT (name) ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT DENY", "SELECT GRANT name"},
			reads:    "SELECT name FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
		},
		{
			name:     "a deny on a table over a column grant the schema keeps",
			held:     []string{"GRANT SELECT (name) ON dbo.t TO cp_reader"},
			declared: "DENY SELECT ON dbo.t TO cp_reader;\nGRANT SELECT (name) ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT DENY", "SELECT GRANT name"},
			reads:    "SELECT name FROM dbo.t",
			denied:   "SELECT id FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'id' of the object 't'.*`,
		},
		{
			name:     "a deny on a table over a grantable column grant",
			held:     []string{"GRANT SELECT (name) ON dbo.t TO cp_reader WITH GRANT OPTION"},
			declared: "DENY SELECT ON dbo.t TO cp_reader;\nGRANT SELECT (name) ON dbo.t TO cp_reader WITH GRANT OPTION;\n",
			want:     []string{"SELECT DENY", "SELECT GRANT_WITH_GRANT_OPTION name"},
			reads:    "SELECT name FROM dbo.t",
			denied:   "SELECT id FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'id' of the object 't'.*`,
		},
		{
			name:     "a deny the schema leaves out",
			held:     []string{"GRANT SELECT ON SCHEMA::dbo TO cp_reader", "DENY SELECT (secret) ON dbo.t TO cp_reader"},
			declared: "GRANT SELECT ON SCHEMA::dbo TO cp_reader;\n",
			want:     []string{"SELECT DENY secret"},
			reads:    "SELECT id, name FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := sqlServerColumnDatabase(c, test.held...)
			file := writeDeclaredSchemaFile(c, "schema.sql", sqlServerColumnBase+test.declared)

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(sqlServerPermissionsOf(c, db, "cp_reader"), qt.DeepEquals, test.want)
			c.Assert(sqlServerAsMember(c, db, test.reads), qt.IsNil)
			c.Assert(sqlServerAsMember(c, db, test.denied), qt.ErrorMatches, test.refusal)

			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// A DENY whose CASCADE would also take a grant the role gave on, and the schema
// keeps, is refused before anything runs, and every role holds what it held.
func TestSchemaApplySQLServerDenyE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	target, db := sqlServerColumnDatabase(c,
		"GRANT SELECT (name) ON dbo.t TO cp_reader WITH GRANT OPTION",
		"CREATE USER cp_other WITHOUT LOGIN",
		"GRANT SELECT (name) ON dbo.t TO cp_other AS cp_reader",
	)
	file := writeDeclaredSchemaFile(c, "schema.sql",
		sqlServerColumnBase+"DENY SELECT ON dbo.t TO cp_reader;\nGRANT SELECT (name) ON dbo.t TO cp_reader WITH GRANT OPTION;\n")

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(applied.Stderr, qt.Contains, `cannot deny SELECT on dbo.t to "cp_reader": "cp_reader" holds it WITH GRANT OPTION, `+
		`so SQL Server denies it with CASCADE, which also takes the SELECT that "cp_reader" granted to "cp_other" (name)`)
	c.Assert(sqlServerPermissionsOf(c, db, "cp_reader"), qt.DeepEquals, []string{"SELECT GRANT_WITH_GRANT_OPTION name"})
	c.Assert(sqlServerPermissionsOf(c, db, "cp_other"), qt.DeepEquals, []string{"SELECT GRANT name"})
}

// `ptah db read` prints the DENY rows of a SQL Server database, and the output
// applied to an empty database denies what the source denies: the DENY is read
// back there with state 'D', a member of the role is refused, and a plan of the
// output against that database is empty. Left out, the output would give a
// role that replays it what a grant on the schema carries, the denied column
// included.
func TestDBReadSQLServerDenyRoundTripE2E(t *testing.T) {
	c := qt.New(t)
	source, _ := sqlServerColumnDatabase(c,
		"GRANT SELECT, UPDATE ON SCHEMA::dbo TO cp_reader",
		"DENY SELECT (secret) ON dbo.t TO cp_reader",
		"DENY UPDATE ON dbo.t TO cp_reader",
	)

	read := runPtahIn(c, "db", "read", "--db-url", source)

	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
	c.Assert(sqlServerPermissionStatements(read.Stdout), qt.DeepEquals, []string{
		"DENY UPDATE ON [dbo].[t] TO [cp_reader];",
		"GRANT SELECT ON SCHEMA::[dbo] TO [cp_reader];",
		"GRANT UPDATE ON SCHEMA::[dbo] TO [cp_reader];",
		"DENY SELECT ([secret]) ON [dbo].[t] TO [cp_reader];",
	})
	file := writeDeclaredSchemaFile(c, "described.sql", read.Stdout)
	target, db := declaredSQLServerDatabase(c)
	declaredSQLServerExec(c, db, "CREATE USER cp_member WITHOUT LOGIN")

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	declaredSQLServerExec(c, db, "INSERT INTO dbo.t VALUES (1, N'a', N's')", "ALTER ROLE cp_reader ADD MEMBER cp_member")
	c.Assert(sqlServerPermissionsOf(c, db, "cp_reader"), qt.DeepEquals, []string{"SELECT DENY secret", "UPDATE DENY"})
	c.Assert(sqlServerAsMember(c, db, "SELECT secret FROM dbo.t"), qt.ErrorMatches,
		`(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`)

	settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}

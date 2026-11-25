//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// sqlServerCollatedColumnDatabase is [sqlServerColumnDatabase] on a scratch
// database of the collation named.
func sqlServerCollatedColumnDatabase(c *qt.C, collation string, held ...string) (string, *sql.DB) {
	c.Helper()
	target := createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
		"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s",
		func(c *qt.C, adminURL, name string) string {
			_, err := connectDevDialect(c, adminURL).ExecContext(c.Context(), "ALTER DATABASE "+name+" COLLATE "+collation)
			c.Assert(err, qt.IsNil)
			return replaceSQLServerDatabase(c, adminURL, name)
		})
	db, err := sql.Open("sqlserver", target)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(db.Close(), qt.IsNil) })
	declaredSQLServerExec(c, db, append(slices.Clone(sqlServerColumnFixture), held...)...)
	return target, db
}

// sqlServerPublicPermissions reads the permissions the fixed role public, the
// principal with principal_id 0, holds on dbo.t, as [sqlServerPermissionsOf]
// spells them.
func sqlServerPublicPermissions(c *qt.C, db *sql.DB) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `
		SELECT pe.permission_name + ' ' + pe.state_desc
		FROM sys.database_permissions AS pe
		WHERE pe.grantee_principal_id = 0 AND pe.class = 1 AND pe.major_id = OBJECT_ID(N'dbo.t')
		ORDER BY 1`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	held := make([]string, 0)
	for rows.Next() {
		var permission string
		c.Assert(rows.Scan(&permission), qt.IsNil)
		held = append(held, permission)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// unmanagedLines is the UNMANAGED lines of a run's diagnostics, nil when
// there are none.
func unmanagedLines(stderr string) []string {
	var lines []string
	for line := range strings.SplitSeq(stderr, "\n") {
		if strings.HasPrefix(line, "UNMANAGED: ") {
			lines = append(lines, line)
		}
	}
	return lines
}

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
// the schema leaves out stays, and the run says so: a removal planned from a
// document that cannot state a DENY would give the role what an administrator
// denied it.
//
// A GRANT or a REVOKE on the whole table takes the column DENY rows of its
// permission, so a plan that grants or revokes on the table denies the column
// again. A column DENY under a grantable table grant takes with CASCADE only
// what the grantee gave on that column, and needs it whether the database
// held the grant or the plan granted it just before.
func TestSchemaApplySQLServerDenyE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		held      []string
		declared  string
		want      []string
		reads     string
		denied    string
		refusal   string
		unmanaged []string
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
			unmanaged: []string{`UNMANAGED: the database denies SELECT on TABLE dbo.t (secret) to "cp_reader", ` +
				"and the schema does not declare the DENY, so the plan does not revoke it; declare it to manage it, or revoke it by hand"},
		},
		{
			name:     "a deny on a column the schema leaves out under a table grant it declares",
			held:     []string{"GRANT SELECT ON dbo.t TO cp_reader", "DENY SELECT (secret) ON dbo.t TO cp_reader"},
			declared: "GRANT SELECT ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT DENY secret", "SELECT GRANT"},
			reads:    "SELECT id, name FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
			unmanaged: []string{`UNMANAGED: the database denies SELECT on TABLE dbo.t (secret) to "cp_reader", ` +
				"and the schema does not declare the DENY, so the plan does not revoke it; declare it to manage it, or revoke it by hand"},
		},
		{
			name:     "a deny on a column after a grant on its table",
			declared: "GRANT SELECT ON dbo.t TO cp_reader;\nDENY SELECT (secret) ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT DENY secret", "SELECT GRANT"},
			reads:    "SELECT id, name FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
		},
		{
			name:     "a grant on a table over a column deny the database holds",
			held:     []string{"DENY SELECT (secret) ON dbo.t TO cp_reader"},
			declared: "GRANT SELECT ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT DENY secret", "SELECT GRANT"},
			reads:    "SELECT id, name FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
			unmanaged: []string{`UNMANAGED: the database denies SELECT on TABLE dbo.t (secret) to "cp_reader", ` +
				"and the schema does not declare the DENY, so the plan does not revoke it; declare it to manage it, or revoke it by hand"},
		},
		{
			name:     "a table grant narrowed to columns over a column deny the database holds",
			held:     []string{"GRANT SELECT ON dbo.t TO cp_reader", "DENY SELECT (secret) ON dbo.t TO cp_reader"},
			declared: "GRANT SELECT (id, name) ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT DENY secret", "SELECT GRANT id", "SELECT GRANT name"},
			reads:    "SELECT id, name FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
			unmanaged: []string{`UNMANAGED: the database denies SELECT on TABLE dbo.t (secret) to "cp_reader", ` +
				"and the schema does not declare the DENY, so the plan does not revoke it; declare it to manage it, or revoke it by hand"},
		},
		{
			name:     "a deny on a column declared under a grantable table grant",
			held:     []string{"GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION"},
			declared: "GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION;\nDENY SELECT (secret) ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT DENY secret", "SELECT GRANT_WITH_GRANT_OPTION"},
			reads:    "SELECT id, name FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
		},
		{
			name:     "a deny on a column under a grantable table grant the plan adds",
			declared: "GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION;\nDENY SELECT (secret) ON dbo.t TO cp_reader;\n",
			want:     []string{"SELECT DENY secret", "SELECT GRANT_WITH_GRANT_OPTION"},
			reads:    "SELECT id, name FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
		},
		{
			name:     "a grantable grant on a table over a column deny the database holds",
			held:     []string{"DENY SELECT (secret) ON dbo.t TO cp_reader"},
			declared: "GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION;\n",
			want:     []string{"SELECT DENY secret", "SELECT GRANT_WITH_GRANT_OPTION"},
			reads:    "SELECT id, name FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
			unmanaged: []string{`UNMANAGED: the database denies SELECT on TABLE dbo.t (secret) to "cp_reader", ` +
				"and the schema does not declare the DENY, so the plan does not revoke it; declare it to manage it, or revoke it by hand"},
		},
		{
			name:     "a grant option added on a table over a column deny the database holds",
			held:     []string{"GRANT SELECT ON dbo.t TO cp_reader", "DENY SELECT (secret) ON dbo.t TO cp_reader"},
			declared: "GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION;\n",
			want:     []string{"SELECT DENY secret", "SELECT GRANT_WITH_GRANT_OPTION"},
			reads:    "SELECT id, name FROM dbo.t",
			denied:   "SELECT secret FROM dbo.t",
			refusal:  `(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`,
			unmanaged: []string{`UNMANAGED: the database denies SELECT on TABLE dbo.t (secret) to "cp_reader", ` +
				"and the schema does not declare the DENY, so the plan does not revoke it; declare it to manage it, or revoke it by hand"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := sqlServerColumnDatabase(c, test.held...)
			file := writeDeclaredSchemaFile(c, "schema.sql", sqlServerColumnBase+test.declared)

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(unmanagedLines(applied.Stderr), qt.DeepEquals, test.unmanaged)
			c.Assert(sqlServerPermissionsOf(c, db, "cp_reader"), qt.DeepEquals, test.want)
			c.Assert(sqlServerAsMember(c, db, test.reads), qt.IsNil)
			c.Assert(sqlServerAsMember(c, db, test.denied), qt.ErrorMatches, test.refusal)

			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// A DENY on a table to a grantee the schema does not manage takes every grant
// of the permission the grantee holds there, and the plan gives none of them
// back. The SQL Server read reports a table grant with a column row of the
// permission under it as a grant on each column, and a plan that granted
// those back after the DENY would leave the grantee reading the table the
// schema denies it -- for a role, and for a database user.
func TestSchemaApplySQLServerDenyToAnUnmanagedGranteeE2E(t *testing.T) {
	tests := []struct {
		name    string
		held    []string
		grantee string
		member  string
	}{
		{
			name: "a role",
			held: []string{
				"CREATE ROLE cp_outside",
				"ALTER ROLE cp_outside ADD MEMBER cp_member",
				"GRANT SELECT ON dbo.t TO cp_outside",
				"GRANT SELECT (name) ON dbo.t TO cp_outside WITH GRANT OPTION",
			},
			grantee: "cp_outside",
			member:  "cp_member",
		},
		{
			name: "a database user",
			held: []string{
				"CREATE USER cp_user WITHOUT LOGIN",
				"GRANT SELECT ON dbo.t TO cp_user",
				"REVOKE SELECT (secret) ON dbo.t FROM cp_user",
			},
			grantee: "cp_user",
			member:  "cp_user",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := sqlServerColumnDatabase(c, test.held...)
			file := writeDeclaredSchemaFile(c, "schema.sql", sqlServerColumnBase+"DENY SELECT ON dbo.t TO "+test.grantee+";\n")

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(sqlServerPermissionsOf(c, db, test.grantee), qt.DeepEquals, []string{"SELECT DENY"})
			c.Assert(sqlServerAs(c, db, test.member, "SELECT name FROM dbo.t"), qt.ErrorMatches,
				`(?s).*The SELECT permission was denied on the object 't'.*`)

			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// A schema directory keeps a column statement a file writes after a statement
// on its table, as the file read alone does: the server keeps both rows, and
// a directory that folded the table statement last would plan the column
// grant away and never settle.
func TestSchemaApplySQLServerDenyDirectoryE2E(t *testing.T) {
	c := qt.New(t)
	target, db := sqlServerColumnDatabase(c)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_base.sql"), []byte(sqlServerColumnBase), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "2_privileges.sql"),
		[]byte("DENY SELECT ON dbo.t TO cp_reader;\nGRANT SELECT (name) ON dbo.t TO cp_reader;\n"), 0o600), qt.IsNil)

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", dir, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(sqlServerPermissionsOf(c, db, "cp_reader"), qt.DeepEquals, []string{"SELECT DENY", "SELECT GRANT name"})
	c.Assert(sqlServerAsMember(c, db, "SELECT name FROM dbo.t"), qt.IsNil)
	c.Assert(sqlServerAsMember(c, db, "SELECT secret FROM dbo.t"), qt.ErrorMatches,
		`(?s).*The SELECT permission was denied on the column 'secret' of the object 't'.*`)

	settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", dir, "--dry-run")

	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// A column DENY under a grantable table grant goes with CASCADE, which takes
// only what the grantee gave on that column: a grant it gave on another
// column stays, so the plan does not refuse over it.
func TestSchemaApplySQLServerColumnDenyCascadeE2E(t *testing.T) {
	c := qt.New(t)
	target, db := sqlServerColumnDatabase(c,
		"GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION",
		"CREATE USER cp_other WITHOUT LOGIN",
		"GRANT SELECT (name) ON dbo.t TO cp_other AS cp_reader",
	)
	file := writeDeclaredSchemaFile(c, "schema.sql",
		sqlServerColumnBase+"GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION;\nDENY SELECT (secret) ON dbo.t TO cp_reader;\n")

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(sqlServerPermissionsOf(c, db, "cp_reader"), qt.DeepEquals, []string{"SELECT DENY secret", "SELECT GRANT_WITH_GRANT_OPTION"})
	c.Assert(sqlServerPermissionsOf(c, db, "cp_other"), qt.DeepEquals, []string{"SELECT GRANT name"})
	c.Assert(sqlServerAs(c, db, "cp_other", "SELECT name FROM dbo.t"), qt.IsNil)

	settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// A GRANT and a DENY to PUBLIC settle, from a file that spells the role PUBLIC
// and through `ptah db read`, which prints the fixed role as [public]: the
// read names the principal with principal_id 0 PUBLIC, as a schema file does,
// so the next plan does not state either again.
func TestSchemaApplySQLServerPublicE2E(t *testing.T) {
	c := qt.New(t)
	target, db := sqlServerColumnDatabase(c)
	file := writeDeclaredSchemaFile(c, "schema.sql",
		sqlServerColumnBase+"GRANT SELECT ON dbo.t TO PUBLIC;\nDENY UPDATE ON dbo.t TO PUBLIC;\n")

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(sqlServerPermissionsOf(c, db, "public"), qt.DeepEquals, []string{"SELECT GRANT", "UPDATE DENY"})
	c.Assert(sqlServerAsMember(c, db, "SELECT secret FROM dbo.t"), qt.IsNil)
	c.Assert(sqlServerAsMember(c, db, "UPDATE dbo.t SET name = N'b'"), qt.ErrorMatches,
		`(?s).*The UPDATE permission was denied on the object 't'.*`)

	settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")

	read := runPtahIn(c, "db", "read", "--db-url", target)

	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
	c.Assert(sqlServerPermissionStatements(read.Stdout), qt.DeepEquals, []string{
		"DENY UPDATE ON [dbo].[t] TO [public];",
		"GRANT SELECT ON [dbo].[t] TO [public];",
	})
	described := writeDeclaredSchemaFile(c, "described.sql", read.Stdout)
	replayed, _ := declaredSQLServerDatabase(c)
	replay := runPtahIn(c, "schema", "apply", "--db-url", replayed, "--schema-file", described, "--auto-approve")

	c.Assert(replay.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", replay.Stdout, replay.Stderr))

	resettled := runPtahIn(c, "schema", "apply", "--db-url", replayed, "--schema-file", described, "--dry-run")

	c.Assert(resettled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", resettled.Stdout, resettled.Stderr))
	c.Assert(resettled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// PUBLIC in a schema file is the fixed role public, the principal with
// principal_id 0, whatever the database's collation and whatever other
// principal is named like it. Measured on SQL Server 2025: [PUBLIC] on a
// case-sensitive database is Msg 15151, or a role of that name, and such a
// database accepts CREATE ROLE [Public] beside public. A DENY to PUBLIC lands
// on public, a member of a role granted DELETE is refused it, the other
// principal keeps what it held, and the next plan is empty.
func TestSchemaApplySQLServerPublicIsTheFixedRoleE2E(t *testing.T) {
	tests := []struct {
		name      string
		collation string
		held      []string
		other     string
		otherHeld []string
	}{
		{
			name:      "a case-sensitive database",
			collation: "Latin1_General_CS_AS",
			held:      []string{"GRANT DELETE ON dbo.t TO cp_reader"},
			other:     "cp_reader",
			otherHeld: []string{"DELETE GRANT"},
		},
		{
			name:      "a case-sensitive database with a role named Public",
			collation: "Latin1_General_CS_AS",
			held:      []string{"GRANT DELETE ON dbo.t TO cp_reader", "CREATE ROLE [Public]", "DENY DELETE ON dbo.t TO [Public]"},
			other:     "Public",
			otherHeld: []string{"DELETE DENY"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := sqlServerCollatedColumnDatabase(c, test.collation, test.held...)
			file := writeDeclaredSchemaFile(c, "schema.sql", sqlServerColumnBase+
				"GRANT DELETE ON dbo.t TO cp_reader;\nGRANT SELECT ON dbo.t TO PUBLIC;\nDENY DELETE ON dbo.t TO PUBLIC;\n")

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(sqlServerPublicPermissions(c, db), qt.DeepEquals, []string{"DELETE DENY", "SELECT GRANT"})
			c.Assert(sqlServerPermissionsOf(c, db, test.other), qt.DeepEquals, test.otherHeld)
			c.Assert(sqlServerAsMember(c, db, "SELECT secret FROM dbo.t"), qt.IsNil)
			c.Assert(sqlServerAsMember(c, db, "DELETE FROM dbo.t"), qt.ErrorMatches,
				`(?s).*The DELETE permission was denied on the object 't'.*`)

			settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// A grantee a schema file writes with trailing spaces is the principal named
// without them: the server compares names with trailing spaces ignored, so
// [cp_reader ] is the role cp_reader and [public ] the fixed role public. Each
// lands on that principal, a member is refused what the DENY withholds, and
// the next plan is empty.
func TestSchemaApplySQLServerTrailingSpaceGranteeE2E(t *testing.T) {
	c := qt.New(t)
	target, db := sqlServerColumnDatabase(c)
	file := writeDeclaredSchemaFile(c, "schema.sql",
		sqlServerColumnBase+"GRANT SELECT ON dbo.t TO [cp_reader  ];\nDENY UPDATE ON dbo.t TO [public ];\n")

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(sqlServerPermissionsOf(c, db, "cp_reader"), qt.DeepEquals, []string{"SELECT GRANT"})
	c.Assert(sqlServerPublicPermissions(c, db), qt.DeepEquals, []string{"UPDATE DENY"})
	c.Assert(sqlServerAsMember(c, db, "SELECT secret FROM dbo.t"), qt.IsNil)
	c.Assert(sqlServerAsMember(c, db, "UPDATE dbo.t SET name = N'b'"), qt.ErrorMatches,
		`(?s).*The UPDATE permission was denied on the object 't'.*`)

	settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// A database holding a role the read cannot keep apart from another principal
// is refused before anything runs, and public holds nothing new. A schema
// names the fixed role PUBLIC, and a case-sensitive database accepts CREATE
// ROLE [PUBLIC] beside it. A comparison trims a grantee's spaces, and any
// database accepts CREATE ROLE [ public], whose DENY read as public's would
// make a DENY to PUBLIC read as applied.
func TestSchemaApplySQLServerPublicIsTheFixedRoleE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		collation string
		held      []string
		wantErr   string
	}{
		{
			name:      "a case-sensitive database with a role named PUBLIC",
			collation: "Latin1_General_CS_AS",
			held:      []string{"CREATE ROLE [PUBLIC]"},
			wantErr:   `sqlserver: read roles: unsupported feature: the database principal "PUBLIC" has the name a schema gives the fixed role public`,
		},
		{
			name:      "a case-insensitive database with a role named with a space before public",
			collation: "SQL_Latin1_General_CP1_CI_AS",
			held:      []string{"CREATE ROLE [ public]", "DENY DELETE ON dbo.t TO [ public]"},
			wantErr:   `sqlserver: read roles: unsupported feature: the database principal " public" has white space around its name that the server keeps and a grant does not`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, db := sqlServerCollatedColumnDatabase(c, test.collation, test.held...)
			file := writeDeclaredSchemaFile(c, "schema.sql", sqlServerColumnBase+"DENY DELETE ON dbo.t TO PUBLIC;\n")

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0)
			c.Assert(applied.Stderr, qt.Contains, test.wantErr)
			c.Assert(sqlServerPublicPermissions(c, db), qt.DeepEquals, make([]string, 0))
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

// A source that holds a grantable table grant with a column DENY under it
// reads as the grant and the DENY, and the output applied to an empty database
// grants the table and then denies the column with CASCADE, which the server
// requires once the grant option is held. The replay holds both rows, and the
// next plan is empty.
func TestDBReadSQLServerGrantableDenyRoundTripE2E(t *testing.T) {
	c := qt.New(t)
	source, _ := sqlServerColumnDatabase(c,
		"GRANT SELECT ON dbo.t TO cp_reader WITH GRANT OPTION",
		"DENY SELECT (secret) ON dbo.t TO cp_reader CASCADE",
	)

	read := runPtahIn(c, "db", "read", "--db-url", source)

	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
	c.Assert(sqlServerPermissionStatements(read.Stdout), qt.DeepEquals, []string{
		"GRANT SELECT ON [dbo].[t] TO [cp_reader] WITH GRANT OPTION;",
		"DENY SELECT ([secret]) ON [dbo].[t] TO [cp_reader];",
	})
	file := writeDeclaredSchemaFile(c, "described.sql", read.Stdout)
	target, db := declaredSQLServerDatabase(c)

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(sqlServerPermissionsOf(c, db, "cp_reader"), qt.DeepEquals, []string{"SELECT DENY secret", "SELECT GRANT_WITH_GRANT_OPTION"})

	settled := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", file, "--dry-run")

	c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
	c.Assert(settled.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// `ptah schema compare` names a DENY the database holds and the schema leaves
// out, as `schema apply` does, beside a result that reports no difference.
func TestSchemaCompareSQLServerNamesAnUnmanagedDenyE2E(t *testing.T) {
	c := qt.New(t)
	target, _ := sqlServerColumnDatabase(c,
		"GRANT SELECT ON SCHEMA::dbo TO cp_reader",
		"DENY SELECT (secret) ON dbo.t TO cp_reader",
	)
	file := writeDeclaredSchemaFile(c, "schema.sql", sqlServerColumnBase+"GRANT SELECT ON SCHEMA::dbo TO cp_reader;\n")

	compared := runPtahIn(c, "schema", "compare", "--db-url", target, "--schema-file", file, "--exit-code")

	c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
	c.Assert(unmanagedLines(compared.Stderr), qt.DeepEquals, []string{
		`UNMANAGED: the database denies SELECT on TABLE dbo.t (secret) to "cp_reader", ` +
			"and the schema does not declare the DENY, so the plan does not revoke it; declare it to manage it, or revoke it by hand",
	})
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

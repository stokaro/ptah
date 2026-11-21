//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// MySQL and MariaDB keep a table or column privilege after DROP TABLE and DROP
// COLUMN, and it applies again to an object created under the same name. The
// tests here hold such a privilege for a role and apply a declaration that
// brings the name back.

// revokeFirstBase is t1 with two rows that share a name, so a UNIQUE key on
// name fails with ERROR 1062.
const revokeFirstBase = "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n" +
	"INSERT INTO t1 VALUES (1, 'a', 's1'), (2, 'a', 's2');\n"

// revokeFirstRole is the role and the one privilege every declaration here
// gives it, SELECT (id) on t1; %[1]s is the role.
const revokeFirstRole = "CREATE ROLE %[1]s;\nGRANT SELECT (id) ON t1 TO %[1]s;\n"

// The plan revokes before it creates. Each declaration takes the held
// privilege from the role, brings the name back, and then fails on a UNIQUE
// key over duplicate rows. A plan that revokes after the CREATE TABLE or ADD
// COLUMN leaves the privilege live once the failure stops it, and a later
// apply that brings the name back gives it to every user holding the role.
// After the failed apply the role holds what both the database and the
// declaration give it, no less and no more, and after the apply that brings
// the name back a user holding the role still cannot read the object the
// privilege was on.
func TestSchemaApplyMySQLFamilyRevokesBeforeANameComesBackE2E(t *testing.T) {
	tests := []struct {
		name    string
		held    []string
		failing string
		final   string
		denied  string
	}{
		{
			name: "a column dropped and added again",
			held: []string{"GRANT SELECT (id, secret) ON `%[1]s`.t1 TO %[2]s"},
			failing: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, UNIQUE KEY t1_name (name));\n" +
				revokeFirstRole,
			final:  "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n" + revokeFirstRole,
			denied: "SELECT secret FROM t1",
		},
		{
			// The table revoke takes SELECT (id) with it, and the plan gives it
			// back before the failing statement, so the role keeps what the
			// declaration keeps.
			name: "a whole table narrowed to a column",
			held: []string{
				"GRANT SELECT ON `%[1]s`.t1 TO %[2]s",
				"GRANT SELECT (id) ON `%[1]s`.t1 TO %[2]s",
			},
			failing: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50), UNIQUE KEY t1_name (name));\n" +
				revokeFirstRole,
			final:  "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n" + revokeFirstRole,
			denied: "SELECT secret FROM t1",
		},
		{
			name: "a table dropped and created again",
			held: []string{
				"GRANT SELECT (id) ON `%[1]s`.t1 TO %[2]s",
				"CREATE TABLE `%[1]s`.t2 (id INT PRIMARY KEY)",
				"GRANT SELECT ON `%[1]s`.t2 TO %[2]s",
				"DROP TABLE `%[1]s`.t2",
			},
			failing: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50), UNIQUE KEY t1_name (name));\n" +
				"CREATE TABLE t2 (id INT PRIMARY KEY);\n" + revokeFirstRole,
			final: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n" +
				"CREATE TABLE t2 (id INT PRIMARY KEY);\n" + revokeFirstRole,
			denied: "SELECT id FROM t2",
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, revokeFirstBase, test.held)

				failed := run.apply(c, test.failing, "--auto-approve")

				c.Assert(failed.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", failed.Stdout))
				c.Assert(failed.Stdout+failed.Stderr, qt.Contains, "Duplicate entry")
				c.Assert(run.privileges(c), qt.DeepEquals, []string{"column t1.id SELECT NO"})

				applied := run.apply(c, test.final, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(run.privileges(c), qt.DeepEquals, []string{"column t1.id SELECT NO"})
				member := run.member(c)
				var id int
				c.Assert(member.QueryRowContext(c.Context(), "SELECT id FROM t1 WHERE id = 1").Scan(&id), qt.IsNil)
				c.Assert(id, qt.Equals, 1)
				_, err := member.ExecContext(c.Context(), test.denied)
				c.Assert(err, qt.ErrorMatches, `(?s).*command denied to user.*`)

				again := run.apply(c, test.final, "--dry-run")

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// A column privilege the declaration keeps and the revoke of its table's
// privilege takes away comes back at once where its column exists, and after
// the CREATE TABLE, CREATE VIEW or ADD COLUMN that brings the column back
// where it does not: the server keeps the privilege of a dropped column, table
// or view, so the read reports it as held, and a GRANT on the missing column
// before it is created fails with ERROR 1054 or 1146 and leaves the role with
// nothing.
// The role ends with the declared privilege, a user holding the role reads
// that column and is refused the others, and the next plan is empty.
func TestSchemaApplyMySQLFamilyGrantsBackAColumnPrivilegeOnceItsColumnExistsE2E(t *testing.T) {
	tests := []struct {
		name   string
		held   []string
		final  string
		want   []string
		reads  string
		denied string
	}{
		{
			name: "a column dropped",
			held: []string{
				"GRANT SELECT ON `%[1]s`.t1 TO %[2]s",
				"GRANT SELECT (secret) ON `%[1]s`.t1 TO %[2]s",
				"ALTER TABLE `%[1]s`.t1 DROP COLUMN secret",
			},
			final:  "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\nCREATE ROLE %[1]s;\nGRANT SELECT (secret) ON t1 TO %[1]s;\n",
			want:   []string{"column t1.secret SELECT NO"},
			reads:  "SELECT secret FROM t1",
			denied: "SELECT id FROM t1",
		},
		{
			name: "a table dropped",
			held: []string{
				"CREATE TABLE `%[1]s`.t2 (id INT PRIMARY KEY)",
				"GRANT SELECT ON `%[1]s`.t2 TO %[2]s",
				"GRANT SELECT (id) ON `%[1]s`.t2 TO %[2]s",
				"DROP TABLE `%[1]s`.t2",
			},
			final: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n" +
				"CREATE TABLE t2 (id INT PRIMARY KEY, name VARCHAR(50));\nCREATE ROLE %[1]s;\nGRANT SELECT (id) ON t2 TO %[1]s;\n",
			want:   []string{"column t2.id SELECT NO"},
			reads:  "SELECT id FROM t2",
			denied: "SELECT name FROM t2",
		},
		{
			name: "a view dropped",
			held: []string{
				"CREATE VIEW `%[1]s`.v1 AS SELECT id, name FROM `%[1]s`.t1",
				"GRANT SELECT ON `%[1]s`.v1 TO %[2]s",
				"GRANT SELECT (id) ON `%[1]s`.v1 TO %[2]s",
				"DROP VIEW `%[1]s`.v1",
			},
			final: "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n" +
				"CREATE VIEW v1 AS SELECT id, name FROM t1;\nCREATE ROLE %[1]s;\nGRANT SELECT (id) ON v1 TO %[1]s;\n",
			want:   []string{"column v1.id SELECT NO"},
			reads:  "SELECT id FROM v1",
			denied: "SELECT name FROM v1",
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, revokeFirstBase, test.held)

				applied := run.apply(c, test.final, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(run.privileges(c), qt.DeepEquals, test.want)
				member := run.member(c)
				_, err := member.ExecContext(c.Context(), test.reads)
				c.Assert(err, qt.IsNil)
				_, err = member.ExecContext(c.Context(), test.denied)
				c.Assert(err, qt.ErrorMatches, `(?s).*command denied to user.*`)

				again := run.apply(c, test.final, "--dry-run")

				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

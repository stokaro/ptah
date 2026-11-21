//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// MySQL and MariaDB take `Name` and `name` for one column, and so does the
// server's in-memory copy of the column privileges, but mysql.columns_priv
// keeps a row per spelling. A privilege granted under one spelling and revoked
// under the other stays in effect while the table loses its row: measured on
// all four servers, a user holding the role reads a column the declaration
// takes away and cannot update one it gives.

// spellingSchema is t1, the role and its column privileges; %[1]s is the role
// and %[2]s the column privileges, one GRANT statement each.
const spellingSchema = "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n" +
	"CREATE VIEW v1 AS SELECT id, name FROM t1;\nCREATE ROLE %[1]s;\n"

// A declaration that spells a column the way the catalog does not matches the
// privilege the catalog holds, and a privilege it adds on that column is
// granted under the catalog's spelling. A user holding the role reads and
// updates the column, and the next plan finds nothing to change.
//
// The user spells the column as the privileges do. Measured on MySQL 8.4.13
// and 26.10.0, a column privilege a user holds through a role serves only a
// statement that spells the column the way the GRANT did: with SELECT (Name)
// granted to the role, `SELECT Name` runs and `SELECT name` is ERROR 1143.
// MariaDB 11.8.9 and 12.3.3 serve both.
func TestSchemaApplyMySQLFamilyColumnGrantSpellingE2E_HappyPath(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRun(c, engine.admin, []string{"GRANT SELECT (Name) ON `%[1]s`.t1 TO %[2]s"})
			declared := spellingSchema + "GRANT SELECT (name) ON t1 TO %[1]s;\nGRANT UPDATE (name) ON t1 TO %[1]s;\n"

			applied := run.apply(c, declared, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Contains, "GRANT UPDATE (`Name`) ON")
			c.Assert(applied.Stdout, qt.Not(qt.Contains), "REVOKE")
			c.Assert(run.privileges(c), qt.DeepEquals, []string{"column t1.Name SELECT NO", "column t1.Name UPDATE NO"})
			member := run.member(c)
			var names int
			c.Assert(member.QueryRowContext(c.Context(), "SELECT COUNT(Name) FROM t1").Scan(&names), qt.IsNil)
			_, err := member.ExecContext(c.Context(), "UPDATE t1 SET Name = 'b'")
			c.Assert(err, qt.IsNil)

			again := run.apply(c, declared, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

// A declaration that spells one column two ways is refused before anything
// runs, and so is a plan that changes a role's privileges on a table where the
// catalog already holds one column under two spellings for it. The role keeps
// what it held.
func TestSchemaApplyMySQLFamilyColumnGrantSpellingE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		held     []string
		declared string
		wantErr  string
		want     []string
	}{
		{
			name:     "two spellings declared",
			declared: spellingSchema + "GRANT SELECT (Name) ON t1 TO %[1]s;\nGRANT UPDATE (name) ON t1 TO %[1]s;\n",
			wantErr:  "spell the column one way",
		},
		{
			name: "two spellings held",
			held: []string{
				"GRANT SELECT (Name) ON `%[1]s`.t1 TO %[2]s",
				"GRANT UPDATE (name) ON `%[1]s`.t1 TO %[2]s",
			},
			declared: spellingSchema + "GRANT UPDATE (name) ON t1 TO %[1]s;\n",
			wantErr:  "revoke the privileges under both spellings by hand",
			want:     []string{"column t1.Name SELECT NO", "column t1.name UPDATE NO"},
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRun(c, engine.admin, test.held)

				applied := run.apply(c, test.declared, "--auto-approve")

				c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s", applied.Stdout))
				c.Assert(applied.Stderr, qt.Contains, test.wantErr)
				c.Assert(run.privileges(c), qt.DeepEquals, test.want)
			})
		}
	}
}

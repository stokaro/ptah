//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// revokeAllSchema is t1 and t2 as the run creates them, and no role.
const revokeAllSchema = "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, secret VARCHAR(50));\n" +
	"CREATE TABLE t2 (id INT PRIMARY KEY);\n"

// revokeAllHeld is what the role holds before each run: ALL on t1, UPDATE on
// one of its columns, and SELECT on t2, which lets a member use the database.
var revokeAllHeld = []string{
	"GRANT ALL ON `%[1]s`.t1 TO %[2]s",
	"GRANT UPDATE (name) ON `%[1]s`.t1 TO %[2]s",
	"GRANT SELECT ON `%[1]s`.t2 TO %[2]s",
}

// memberOfNothingOnT1 are statements a member of a role holding nothing on t1
// is refused, one per kind of privilege ALL gives there.
var memberOfNothingOnT1 = []string{
	"SELECT id FROM t1",
	"UPDATE t1 SET name = 'b'",
	"ALTER TABLE t1 ADD COLUMN extra INT",
	"CREATE INDEX t1_name ON t1 (name)",
	"DROP TABLE t1",
}

// A REVOKE ALL takes every privilege the role holds on the table, whatever the
// server calls it, from a role the schema creates and from one it does not.
// With ALL read as PostgreSQL's table list, the plan revokes DELETE, INSERT,
// REFERENCES, SELECT, TRIGGER and UPDATE and leaves ALTER, CREATE, CREATE VIEW,
// DROP, INDEX and SHOW VIEW, and DELETE HISTORY on MariaDB: a member alters,
// indexes and drops the table, and the next plan reads as synced. Measured on
// MySQL 8.4.13 and MariaDB 11.8.9. A grant before the revoke in the file is
// taken with the rest, and a grant after it is the one privilege the role keeps
// on the table.
func TestSchemaApplyMySQLFamilyRevokeAllE2E(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     []string
		allowed  []string
		refused  []string
	}{
		{
			name:     "a role the schema does not create",
			declared: revokeAllSchema + "REVOKE ALL ON t1 FROM %[1]s;\n",
			want:     []string{"table t2 SELECT NO"},
			allowed:  []string{"SELECT id FROM t2"},
			refused:  memberOfNothingOnT1,
		},
		{
			name:     "ALL PRIVILEGES",
			declared: revokeAllSchema + "REVOKE ALL PRIVILEGES ON t1 FROM %[1]s;\n",
			want:     []string{"table t2 SELECT NO"},
			allowed:  []string{"SELECT id FROM t2"},
			refused:  memberOfNothingOnT1,
		},
		{
			name:     "a role the schema creates",
			declared: revokeAllSchema + "CREATE ROLE %[1]s;\nGRANT SELECT ON t2 TO %[1]s;\nREVOKE ALL ON t1 FROM %[1]s;\n",
			want:     []string{"table t2 SELECT NO"},
			allowed:  []string{"SELECT id FROM t2"},
			refused:  memberOfNothingOnT1,
		},
		{
			name:     "a grant before the revoke",
			declared: revokeAllSchema + "GRANT SELECT ON t2 TO %[1]s;\nGRANT SELECT ON t1 TO %[1]s;\nREVOKE ALL ON t1 FROM %[1]s;\n",
			want:     []string{"table t2 SELECT NO"},
			allowed:  []string{"SELECT id FROM t2"},
			refused:  memberOfNothingOnT1,
		},
		{
			name:     "a grant after the revoke",
			declared: revokeAllSchema + "REVOKE ALL ON t1 FROM %[1]s;\nGRANT SELECT ON t1 TO %[1]s;\n",
			want:     []string{"table t1 SELECT NO", "table t2 SELECT NO"},
			allowed:  []string{"SELECT id FROM t2", "SELECT id FROM t1"},
			refused:  memberOfNothingOnT1[1:],
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, revokeAllSchema, revokeAllHeld)

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

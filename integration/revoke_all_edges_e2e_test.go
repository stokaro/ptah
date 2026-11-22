//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// revokedFromT1 applies declared with the extra arguments and asserts that the
// role keeps SELECT on t2 and nothing on t1, that a member is refused every
// statement on t1, and that the next plan is empty.
func revokedFromT1(c *qt.C, run columnGrantRun, declared string, args ...string) {
	c.Helper()
	applied := run.apply(c, declared, append([]string{"--auto-approve"}, args...)...)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(run.privileges(c), qt.DeepEquals, []string{"table t2 SELECT NO"})
	member := run.member(c)
	for _, statement := range memberOfNothingOnT1 {
		_, err := member.ExecContext(c.Context(), statement)
		c.Assert(err, qt.ErrorMatches, `(?s).*command denied to user.*`, qt.Commentf("%s", statement))
	}

	again := run.apply(c, declared, append([]string{"--dry-run"}, args...)...)

	c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// A YAML revoke of ALL PRIVILEGES takes every privilege the role holds on the
// table, as ALL does. Read as a privilege of that name, it matches no row the
// catalog reports: measured on MySQL 8.4.13 and MariaDB 11.8.9, the plan
// revokes nothing, a member keeps every privilege on t1, and the next plan
// reads as synced.
func TestSchemaApplyMySQLFamilyRevokeAllPrivilegesYAMLE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, engine.admin, revokeAllSchema, revokeAllHeld)
			revokes := filepath.Join(c.TempDir(), "revokes.yaml")
			c.Assert(os.WriteFile(revokes, fmt.Appendf(nil,
				"revokes:\n  everything:\n    role: %s\n    privileges: [ALL PRIVILEGES]\n    on_table: t1\n", run.role), 0o600), qt.IsNil)

			revokedFromT1(c, run, revokeAllSchema+"GRANT SELECT ON t2 TO %[1]s;\n", "--schema-file", revokes)
		})
	}
}

// A REVOKE ALL on a table the plan creates, where the role holds privileges a
// dropped table of the same name left behind, revokes each of them once.
// MySQL and MariaDB keep a dropped table's privileges under its name, so the
// catalog reports them on the new table. With the created-table REVOKE ALL
// planned beside the per-privilege revokes, it runs after them, and where
// they took everything the apply fails with Error 1147: measured on MySQL
// 8.4.13 and MariaDB 11.8.9 for the table privileges alone. A column
// privilege left behind is revoked on its own.
func TestSchemaApplyMySQLFamilyRevokeAllOverDroppedTableE2E(t *testing.T) {
	tests := []struct {
		name string
		held []string
	}{
		{
			name: "table privileges",
			held: []string{
				"GRANT SELECT, INSERT ON `%[1]s`.t1 TO %[2]s",
				"GRANT SELECT ON `%[1]s`.t2 TO %[2]s",
				"DROP TABLE `%[1]s`.t1",
			},
		},
		{
			name: "table and column privileges",
			held: []string{
				"GRANT SELECT, INSERT ON `%[1]s`.t1 TO %[2]s",
				"GRANT UPDATE (name) ON `%[1]s`.t1 TO %[2]s",
				"GRANT SELECT ON `%[1]s`.t2 TO %[2]s",
				"DROP TABLE `%[1]s`.t1",
			},
		},
	}
	for _, engine := range mysqlServerEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, revokeAllSchema, test.held)

				revokedFromT1(c, run, revokeAllSchema+"REVOKE ALL ON t1 FROM %[1]s;\n")
			})
		}
	}
}

// A REVOKE ALL on a ClickHouse table the role reads through a grant on the
// whole database is refused: the catalog reports the grant on the database
// alone, so the plan finds nothing on the table to revoke. Measured on
// ClickHouse 26.10, the plan revokes nothing, a member keeps reading t1, and
// the next plan reads as synced. For a role the schema creates the plan
// removes the undeclared database grant, and the revoke holds.
func TestSchemaApplyClickHouseRevokeAllUnderDatabaseGrantE2E(t *testing.T) {
	const table = "CREATE TABLE t1 (id UInt32, name String) ENGINE = MergeTree ORDER BY id;\n"

	t.Run("a role the schema does not create", func(t *testing.T) {
		c := qt.New(t)
		server := newClickHouseRevokeAllServer(c, "GRANT SELECT ON {role}.* TO {role}")

		applied := server.apply(c, table+"REVOKE ALL ON t1 FROM {role};\n", "--auto-approve")

		c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
		c.Assert(applied.Stderr, qt.Matches, `(?s).*role "`+server.role+`" holds SELECT on `+server.role+`\.\*, which covers `+
			server.role+`\.t1, so a revoke on the table cannot take it.*`)
		c.Assert(server.privileges(c), qt.DeepEquals, []string{"SELECT 0"})
	})

	t.Run("a role the schema creates", func(t *testing.T) {
		c := qt.New(t)
		server := newClickHouseRevokeAllServer(c, "GRANT SELECT ON {role}.* TO {role}")

		server.converges(c, table+"CREATE ROLE {role};\nREVOKE ALL ON t1 FROM {role};\n", nil, nil,
			[]string{"SELECT id FROM t1"}, `(?s).*code: 497, .*Not enough privileges.*`)
	})
}

//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// grantOptionTable is the table the grant option tests grant on.
const grantOptionTable = "CREATE TABLE t1 (id INT PRIMARY KEY);\n"

// grantOptionHeld gives the role SELECT on t1 with the grant option.
var grantOptionHeld = []string{"GRANT SELECT ON `%[1]s`.t1 TO %[2]s WITH GRANT OPTION"}

// A declared REVOKE GRANT OPTION FOR takes the grant option a MySQL or
// MariaDB role holds on t1 and leaves the privilege, whether or not the schema
// creates the role. The plan names no privilege, since the option belongs to
// the grantee and the table on these engines. The next plan finds nothing to
// change.
func TestSchemaApplyMySQLFamilyTakesADeclaredGrantOptionE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		for _, test := range []struct {
			name  string
			roles string
		}{
			{name: "a role the schema does not create", roles: ""},
			{name: "a role the schema creates", roles: "CREATE ROLE %[1]s;\n"},
		} {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, grantOptionTable, grantOptionHeld)
				c.Assert(run.privileges(c), qt.DeepEquals, []string{"table t1 SELECT YES"})
				desired := grantOptionTable + test.roles +
					"GRANT SELECT ON t1 TO %[1]s WITH GRANT OPTION;\n" +
					"REVOKE GRANT OPTION FOR SELECT ON t1 FROM %[1]s;\n"

				applied := run.apply(c, desired, "--auto-approve")
				again := run.apply(c, desired, "--dry-run")

				c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(applied.Stdout, qt.Contains, "REVOKE GRANT OPTION ON ")
				c.Assert(run.privileges(c), qt.DeepEquals, []string{"table t1 SELECT NO"})
				c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
				c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			})
		}
	}
}

// The control for the declared revoke: a GRANT without WITH GRANT OPTION alone
// says nothing about the grant option of a role the schema does not create, so
// the role keeps it and the plan is empty.
func TestSchemaApplyMySQLFamilyLeavesTheGrantOptionOfARoleTheSchemaDoesNotCreateE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, engine.admin, grantOptionTable, grantOptionHeld)

			settled := run.apply(c, grantOptionTable+"GRANT SELECT ON t1 TO %[1]s;\n", "--dry-run")

			c.Assert(settled.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", settled.Stdout, settled.Stderr))
			c.Assert(settled.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(run.privileges(c), qt.DeepEquals, []string{"table t1 SELECT YES"})
		})
	}
}

// On MySQL and MariaDB the grant option belongs to the grantee at the whole
// object, so a revoke of it for SELECT takes it from every privilege the role
// holds on t1. Where the role holds another privilege on t1 with the option,
// or a privilege on one of its columns, that the schema keeps grantable, the
// plan is refused and names it. What the role holds is read back unchanged.
func TestSchemaApplyMySQLFamilyRefusesAGrantOptionRevokeThatTakesMoreE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		for _, test := range []struct {
			name     string
			held     []string
			wantHeld []string
			wantErr  string
		}{
			{
				name:     "another privilege on the table",
				held:     []string{"GRANT SELECT, INSERT ON `%[1]s`.t1 TO %[2]s WITH GRANT OPTION"},
				wantHeld: []string{"table t1 INSERT YES", "table t1 SELECT YES"},
				wantErr:  "the revoke also takes it from INSERT, which the schema keeps grantable",
			},
			{
				name:     "a privilege on a column",
				held:     []string{"GRANT SELECT ON `%[1]s`.t1 TO %[2]s WITH GRANT OPTION", "GRANT UPDATE (id) ON `%[1]s`.t1 TO %[2]s"},
				wantHeld: []string{"table t1 SELECT YES", "column t1.id UPDATE YES"},
				wantErr:  "the revoke also takes it from UPDATE (id), which the schema keeps grantable",
			},
		} {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				run := newColumnGrantRunFrom(c, engine.admin, grantOptionTable, test.held)
				c.Assert(run.privileges(c), qt.DeepEquals, test.wantHeld)

				applied := run.apply(c, grantOptionTable+"GRANT SELECT ON t1 TO %[1]s WITH GRANT OPTION;\n"+
					"REVOKE GRANT OPTION FOR SELECT ON t1 FROM %[1]s;\n", "--auto-approve")

				c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
				c.Assert(applied.Stderr, qt.Contains, `cannot revoke the grant option for SELECT on `+run.database+`.t1 from "`+run.role+`": `)
				c.Assert(applied.Stderr, qt.Contains, test.wantErr)
				c.Assert(run.privileges(c), qt.DeepEquals, test.wantHeld)
			})
		}
	}
}

// The control: a schema that takes the option for each privilege the role
// holds on t1 takes it at once, and the next plan is empty.
func TestSchemaApplyMySQLFamilyTakesTheGrantOptionForEveryPrivilegeE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			run := newColumnGrantRunFrom(c, engine.admin, grantOptionTable,
				[]string{"GRANT SELECT, INSERT ON `%[1]s`.t1 TO %[2]s WITH GRANT OPTION"})
			desired := grantOptionTable + "GRANT SELECT, INSERT ON t1 TO %[1]s WITH GRANT OPTION;\n" +
				"REVOKE GRANT OPTION FOR SELECT, INSERT ON t1 FROM %[1]s;\n"

			applied := run.apply(c, desired, "--auto-approve")
			again := run.apply(c, desired, "--dry-run")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(run.privileges(c), qt.DeepEquals, []string{"table t1 INSERT NO", "table t1 SELECT NO"})
			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
		})
	}
}

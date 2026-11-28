//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// unreadHolderTables are the tables the revoke tests read back, and
// unreadHolderSchema the schema file that holds them and revokes SELECT on a
// from the role {holder}.
const (
	unreadHolderTables = "CREATE TABLE a (id int PRIMARY KEY);\nCREATE TABLE b (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY);\n"
	unreadHolderSchema = unreadHolderTables + "REVOKE SELECT ON a FROM {holder};\n"
)

// unreadHolderRun is one database with the tables a, b and c, the role holder
// holding SELECT on a, and the statements of a row run on the server.
type unreadHolderRun struct {
	server   mysqlFamilyServer
	database string
	suffix   string
	holder   string
}

func newUnreadHolderRun(c *qt.C, prefix string) unreadHolderRun {
	c.Helper()
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	database := server.database(c, prefix)
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	run := unreadHolderRun{server: server, database: database, suffix: suffix, holder: "uh" + suffix}
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+granteeIdentifier(run.holder))
		c.Check(err, qt.IsNil)
	})
	partialRevokeExec(c, server, []string{
		"CREATE TABLE " + granteeIdentifier(database) + ".a (id int PRIMARY KEY)",
		"CREATE TABLE " + granteeIdentifier(database) + ".b (id int PRIMARY KEY)",
		"CREATE TABLE " + granteeIdentifier(database) + ".c (id int PRIMARY KEY)",
		"CREATE ROLE " + granteeIdentifier(run.holder),
		"GRANT SELECT ON " + granteeIdentifier(database) + ".a TO " + granteeIdentifier(run.holder),
	})
	return run
}

// spell replaces {db}, {sfx} and {holder} in statement.
func (r unreadHolderRun) spell(statement string) string {
	return strings.NewReplacer("{db}", granteeIdentifier(r.database), "{sfx}", r.suffix,
		"{holder}", granteeIdentifier(r.holder)).Replace(statement)
}

// apply runs `ptah schema apply` of unreadHolderSchema against the database.
func (r unreadHolderRun) apply(c *qt.C) clirun.Result {
	c.Helper()
	file := writeDeclaredSchemaFile(c, "revoke.sql", strings.ReplaceAll(unreadHolderSchema, "{holder}", r.holder))
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute},
		"schema", "apply", "--db-url", r.server.url("mysql", r.database), "--schema-file", file, "--auto-approve")
}

// TestSchemaApplyRefusesATableRevokeAnUnreadPrincipalKeepsE2E grants the role
// holder a principal whose privileges the read does not read in full, holding
// SELECT on the whole database, beside holder's own SELECT on a. A REVOKE of
// SELECT on a from holder takes only holder's own grant, and a member of
// holder still reads a through the principal: measured on MySQL 8.4.13 and
// 26.10.0. The apply is refused with the read's cause, holder keeps its
// grant, a member reads every table, and `db read` prints the cause as a
// note.
func TestSchemaApplyRefusesATableRevokeAnUnreadPrincipalKeepsE2E(t *testing.T) {
	tests := []struct {
		name string
		// drop is the accounts the row creates; setup creates and grants them.
		drop, setup []string
		// principal is how the read names the unread principal.
		principal string
		wantCause string
	}{
		{
			// The login holds a role, so a membership names it, and it
			// reads as the role's name.
			name: "a name two accounts share",
			drop: []string{"`nm{sfx}@localhost`@'%'", "`nm{sfx}`@'localhost'", "`nmx{sfx}`@'%'"},
			setup: []string{
				"CREATE ROLE `nm{sfx}@localhost`@'%', `nmx{sfx}`", "CREATE USER `nm{sfx}`@'localhost' ACCOUNT LOCK",
				"GRANT SELECT ON {db}.* TO `nm{sfx}@localhost`@'%'", "GRANT `nm{sfx}@localhost`@'%' TO {holder}",
				"GRANT `nmx{sfx}` TO `nm{sfx}`@'localhost'",
			},
			principal: "nm{sfx}@localhost",
			wantCause: "the accounts 'nm{sfx}@localhost'@'%' and 'nm{sfx}'@'localhost' read as this one name, so the read leaves its privileges out",
		},
		{
			name:      "a login on another host",
			drop:      []string{"`lg{sfx}`@'localhost'"},
			setup:     []string{"CREATE USER `lg{sfx}`@'localhost' ACCOUNT LOCK", "GRANT SELECT ON {db}.* TO `lg{sfx}`@'localhost'", "GRANT `lg{sfx}`@'localhost' TO {holder}"},
			principal: "lg{sfx}@localhost",
			wantCause: "it is the account 'lg{sfx}'@'localhost', and the read reads the privileges of roles and of accounts on host % alone",
		},
		{
			name:      "a role on another host",
			drop:      []string{"`rh{sfx}`@'localhost'"},
			setup:     []string{"CREATE ROLE `rh{sfx}`@'localhost'", "GRANT SELECT ON {db}.* TO `rh{sfx}`@'localhost'", "GRANT `rh{sfx}`@'localhost' TO {holder}"},
			principal: "rh{sfx}@localhost",
			wantCause: "it is the account 'rh{sfx}'@'localhost', and the read reads the privileges of roles and of accounts on host % alone",
		},
		{
			name:      "a login on host %",
			drop:      []string{"`lp{sfx}`@'%'"},
			setup:     []string{"CREATE USER `lp{sfx}`@'%' ACCOUNT LOCK", "GRANT SELECT ON {db}.* TO `lp{sfx}`@'%'", "GRANT `lp{sfx}`@'%' TO {holder}"},
			principal: "lp{sfx}",
			wantCause: "it is a login, and the read reads a login's grants on the databases it covers, not its global privileges or its grants on database name patterns",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newUnreadHolderRun(c, "unread_holder")
			c.Cleanup(func() {
				for _, account := range test.drop {
					_, err := run.server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+run.spell(account))
					c.Check(err, qt.IsNil)
				}
			})
			for _, statement := range test.setup {
				partialRevokeExec(c, run.server, []string{run.spell(statement)})
			}
			held := grantRows(c, run.server.admin, "SHOW GRANTS FOR "+granteeIdentifier(run.holder))
			principal, cause := run.spell(test.principal), run.spell(test.wantCause)

			applied := run.apply(c)
			read := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute},
				"db", "read", "--db-url", run.server.url("mysql", run.database))

			c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stderr, qt.Contains, fmt.Sprintf("role %q holds %s, whose privileges the read did not read in full: %s; "+
				"so a revoke of SELECT on table a from %q cannot be shown to take the privilege", run.holder, principal, cause, run.holder))
			c.Assert(grantRows(c, run.server.admin, "SHOW GRANTS FOR "+granteeIdentifier(run.holder)), qt.DeepEquals, held)
			c.Assert(loginReads(c, run.server, run.database, "uhm"+run.suffix, []string{granteeIdentifier(run.holder)}), qt.Equals, 3)
			c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
			c.Assert(read.Stderr, qt.Contains, principal+", held by "+run.holder+": "+cause)
		})
	}
}

// TestSchemaApplyRevokesATableGrantNoUnreadPrincipalKeepsE2E is the control:
// the role holder holds no other principal, so the revoke is applied and a
// member of holder reads no table.
func TestSchemaApplyRevokesATableGrantNoUnreadPrincipalKeepsE2E(t *testing.T) {
	c := qt.New(t)
	run := newUnreadHolderRun(c, "unread_holder_control")

	applied := run.apply(c)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Contains, "REVOKE SELECT ON `"+run.database+"`.`a` FROM `"+run.holder+"`")
	c.Assert(loginReads(c, run.server, run.database, "uhm"+run.suffix, []string{granteeIdentifier(run.holder)}), qt.Equals, 0)
}

// TestSchemaApplyRefusesATableRevokeBesideARoleNamedPublicAndMandatoryRolesE2E
// creates a MySQL role named PUBLIC beside the mandatory role mand6, which
// holds SELECT on the whole database. A role named PUBLIC hides which roles
// every account holds, so the read cannot state that every account holds
// mand6, and a REVOKE of SELECT on a from holder takes only holder's own
// grant: a member reads a through mand6 after SET ROLE ALL. The apply is
// refused naming mand6, holder keeps its grant, a member reads every table,
// and `db read` prints the cause as a note.
func TestSchemaApplyRefusesATableRevokeBesideARoleNamedPublicAndMandatoryRolesE2E(t *testing.T) {
	c := qt.New(t)
	run := newUnreadHolderRun(c, "unread_mandatory")
	mandatory := "mand6" + run.suffix
	c.Cleanup(func() {
		_, err := run.server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+granteeIdentifier(mandatory)+", PUBLIC")
		c.Check(err, qt.IsNil)
	})
	partialRevokeExec(c, run.server, []string{
		"CREATE ROLE PUBLIC, " + granteeIdentifier(mandatory),
		"GRANT SELECT ON " + granteeIdentifier(run.database) + ".* TO " + granteeIdentifier(mandatory),
	})
	withMandatoryRoles(c, run.server, mandatory)
	held := grantRows(c, run.server.admin, "SHOW GRANTS FOR "+granteeIdentifier(run.holder))
	const cause = "a role named PUBLIC hides which roles every account holds, since each membership a read records for PUBLIC is that role's"

	applied := run.apply(c)
	read := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute},
		"db", "read", "--db-url", run.server.url("mysql", run.database))

	c.Assert(applied.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stderr, qt.Contains, "every account holds the roles "+mandatory+" whose memberships the read does not state: "+cause+
		"; so a revoke of SELECT on table a from \""+run.holder+"\" cannot be shown to take the privilege")
	c.Assert(grantRows(c, run.server.admin, "SHOW GRANTS FOR "+granteeIdentifier(run.holder)), qt.DeepEquals, held)
	c.Assert(loginReads(c, run.server, run.database, "uhm"+run.suffix, []string{granteeIdentifier(run.holder)}), qt.Equals, 3)
	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", read.Stderr))
	c.Assert(read.Stderr, qt.Contains, "note: every account holds the roles "+mandatory+" whose memberships this read does not state: "+cause)
}

//go:build integration

package mysqlaccount_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// preflightAccount is an account a migration runs as. A grant may name {db} and
// {literal}, as [fixture.newAccount] fills them, {left} for the name of the database
// the migration runs on, and {role} for a role of the fixture's.
type preflightAccount struct {
	name   string
	grants []string
	// roleGrants are the grants of the role.
	roleGrants []string
	// roleActive makes the role active on login.
	roleActive bool
	// mysqlHides is what the row is there to prove: MySQL hides the fixture's
	// trigger from the account.
	mysqlHides bool
}

// runOnly is what an account needs to run the migrations here, without TRIGGER.
const runOnly = "SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, ALTER, INDEX"

// preflightAccounts cover how a session comes to hold TRIGGER on a database, each
// next to what looks like holding it and does not.
var preflightAccounts = []preflightAccount{
	{name: "a literal row", grants: []string{"GRANT ALL ON {db}.* TO {account}"}},
	{name: "a global row", grants: []string{"GRANT ALL ON *.* TO {account}"}},
	{name: "a trailing wildcard", grants: []string{"GRANT ALL ON `{left}%`.* TO {account}"}},
	{name: "escaped underscores and a wildcard", grants: []string{"GRANT ALL ON " + everyDatabase + ".* TO {account}"}},
	{
		name:       "an active role holding a wildcard row",
		grants:     []string{"GRANT {role} TO {account}"},
		roleGrants: []string{"GRANT ALL ON `{left}%`.* TO {role}"},
		roleActive: true,
	},
	{
		name:       "a role that is not active",
		grants:     []string{"GRANT " + runOnly + " ON {db}.* TO {account}", "GRANT {role} TO {account}"},
		roleGrants: []string{"GRANT TRIGGER ON {db}.* TO {role}"},
		mysqlHides: true,
	},
	{name: "no TRIGGER at all", grants: []string{"GRANT " + runOnly + " ON {db}.* TO {account}"}, mysqlHides: true},
	{
		// The server uses the most specific row that matches, and not all of them.
		name:       "a wildcard row a literal row hides",
		grants:     []string{"GRANT ALL ON `{left}%`.* TO {account}", "GRANT " + runOnly + " ON {literal}.* TO {account}"},
		mysqlHides: true,
	},
	{
		// A table with no grant of its own is one whose triggers stay hidden.
		name:       "TRIGGER on another table",
		grants:     []string{"GRANT " + runOnly + " ON {db}.* TO {account}", "GRANT TRIGGER ON {db}.`parent` TO {account}"},
		mysqlHides: true,
	},
}

// TestTheTransactionPreflightReadsAGrantAsTheServerDoesE2E asks the server whether
// the account sees a trigger the administrator created, and holds both binaries to
// that answer: `--tx-mode file` applies a migration for an account the catalog hides
// nothing from, and refuses one it hides a trigger from.
//
// MariaDB shows a trigger's identity without the privilege, so it has no such
// refusal. Its rows are the control that the grants themselves let the run through.
func TestTheTransactionPreflightReadsAGrantAsTheServerDoesE2E(t *testing.T) {
	for _, s := range servers() {
		for _, account := range preflightAccounts {
			t.Run(s.name+", "+account.name, func(t *testing.T) {
				c := qt.New(t)
				f := newFixture(c, s)
				name := f.preflightAccount(c, s, account)
				url := f.url(name, f.left)

				visible := f.seesTrigger(c, s, name, "child_trg")
				c.Assert(visible, qt.Equals, s.mariaDB || !account.mysqlHides)

				f.assertHeldToTheCatalog(c, runNativeMigration(c, url, "applied_natively"), "applied_natively", 2, visible)
				f.assertHeldToTheCatalog(c, runCompatMigration(c, url, "applied_compat"), "applied_compat", 1, visible)
			})
		}
	}
}

// assertHeldToTheCatalog holds a run to what the server showed the account: a run
// applies for an account that sees the trigger, and is refused with its binary's
// exit code, having created nothing, for one that does not.
func (f fixture) assertHeldToTheCatalog(c *qt.C, got clirun.Result, table string, refused int, visible bool) {
	c.Helper()
	comment := qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr)
	c.Assert(got.ExitCode, qt.Equals, map[bool]int{true: 0, false: refused}[visible], comment)
	c.Assert(got.Stderr, qt.Contains, map[bool]string{
		false: "tx-mode file requires the TRIGGER privilege at database or global scope on MySQL",
	}[visible], comment)
	c.Assert(f.tableCount(c, table), qt.Equals, map[bool]int{true: 1, false: 0}[visible], comment)
}

// preflightAccount creates the account and its role, and returns the account's name.
func (f fixture) preflightAccount(c *qt.C, s server, account preflightAccount) string {
	c.Helper()
	role := f.newRole(c, "prefl")
	names := strings.NewReplacer("{left}", f.left, "{role}", role)
	for _, grant := range account.roleGrants {
		f.exec(c, grantsOn(f, names.Replace(grant))...)
	}
	grants := make([]string, 0, len(account.grants))
	for _, grant := range account.grants {
		grants = append(grants, names.Replace(grant))
	}
	name := f.newAccount(c, "txpf", grants...)
	active := map[bool][]string{true: {defaultRole(s, role, name)}}
	f.exec(c, active[account.roleActive]...)
	return name
}

// seesTrigger asks the server, as account, whether it shows the trigger.
func (f fixture) seesTrigger(c *qt.C, s server, account, trigger string) bool {
	c.Helper()
	_, address, found := strings.Cut(dbtarget.DriverDSN(c, s.admin), "@")
	c.Assert(found, qt.IsTrue)
	conn, err := sql.Open("mysql", account+":"+accountPassword+"@"+address[:strings.LastIndex(address, "/")+1]+f.left)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(conn.Close(), qt.IsNil) }()
	var count int
	err = conn.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM information_schema.triggers WHERE trigger_schema = ? AND trigger_name = ?",
		f.left, trigger).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

// tableCount reads the database through the administrator's connection, which a
// grant cannot hide a table from.
func (f fixture) tableCount(c *qt.C, table string) int {
	c.Helper()
	var count int
	err := f.admin.QueryRowContext(c.Context(),
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
		f.left, table).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count
}

// runNativeMigration runs one migration creating table through `ptah migrations up`.
func runNativeMigration(c *qt.C, url, table string) clirun.Result {
	c.Helper()
	dir := c.TempDir()
	up := "CREATE TABLE " + table + " (id INT PRIMARY KEY);\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "1_probe.up.sql"), []byte(up), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_probe.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, "migrations", "up", "--db-url", url, "--migrations-dir", dir)
}

// runCompatMigration runs one migration creating table through `ptah-compat migrate
// apply`. The fixture's tables make the database dirty, which the run allows.
func runCompatMigration(c *qt.C, url, table string) clirun.Result {
	c.Helper()
	dir := c.TempDir()
	up := "CREATE TABLE " + table + " (id INT PRIMARY KEY);\n"
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_probe.sql"), []byte(up), 0o600), qt.IsNil)
	address := "file://" + filepath.ToSlash(dir)
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir}, "migrate", "hash", "--dir", address)
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", hashed.Stderr))
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: dir},
		"migrate", "apply", "--url", url, "--dir", address, "--tx-mode", "file", "--allow-dirty")
}

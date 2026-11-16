//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A plan that keeps a view's definer names it with DEFINER=, and a migration
// file may name one by hand. The server refuses such a statement with ERROR
// 1227 when the account applying it lacks the privilege to name another
// account, and MySQL-family DDL runs outside a transaction, so the apply paths
// ask before the first statement (stokaro/ptah#6100).

// definerPassword is the password of the accounts these tests create.
const definerPassword = "ptah-definer-pw" // #nosec G101 -- the password of scratch accounts the tests drop, not a credential

// definerPrivilege is the global privilege each engine checks before a
// statement names another account as a definer.
var definerPrivilege = map[string]string{"mysql": "SET_ANY_DEFINER", "mariadb": "SET USER"}

// definerAccounts are an account a view is defined for and a deploy account,
// each with a password and every privilege on one database.
type definerAccounts struct {
	owner, ownerURL       string
	deployer, deployerURL string
}

// newDefinerAccounts creates a deploy account for database and, when owner
// names no account yet, an owner account too; owner given is an account the
// caller created, which receives a password. Both are dropped after the test.
// privilege, when given, is granted to the deploy account on *.*.
func newDefinerAccounts(c *qt.C, scratch mysqlScratch, database, owner, privilege string) definerAccounts {
	c.Helper()
	suffix := time.Now().UnixNano()
	ownerUser := strings.TrimSuffix(owner, "@%")
	var statements []string
	if ownerUser == "" {
		ownerUser = fmt.Sprintf("ptah_own_%d", suffix)
		statements = append(statements, "CREATE USER '"+ownerUser+"'@'%' IDENTIFIED BY '"+definerPassword+"'")
		c.Cleanup(func() { dropDefinerAccount(c, scratch, ownerUser) })
	}
	deployer := fmt.Sprintf("ptah_dep_%d", suffix)
	c.Cleanup(func() { dropDefinerAccount(c, scratch, deployer) })
	statements = append(statements,
		"ALTER USER '"+ownerUser+"'@'%' IDENTIFIED BY '"+definerPassword+"'",
		"GRANT ALL ON `"+database+"`.* TO '"+ownerUser+"'@'%'",
		"CREATE USER '"+deployer+"'@'%' IDENTIFIED BY '"+definerPassword+"'",
		"GRANT ALL ON `"+database+"`.* TO '"+deployer+"'@'%'",
	)
	if privilege != "" {
		statements = append(statements, "GRANT "+privilege+" ON *.* TO '"+deployer+"'@'%'")
	}
	for _, statement := range statements {
		_, err := scratch.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	url := replaceMySQLDatabaseName(c, scratch.adminURL, database)
	return definerAccounts{
		owner:       ownerUser + "@%",
		ownerURL:    replaceMySQLCredentials(c, url, ownerUser, definerPassword),
		deployer:    deployer + "@%",
		deployerURL: replaceMySQLCredentials(c, url, deployer, definerPassword),
	}
}

func dropDefinerAccount(c *qt.C, scratch mysqlScratch, user string) {
	c.Helper()
	_, err := scratch.admin.ExecContext(context.Background(), "DROP USER IF EXISTS '"+user+"'@'%'")
	c.Check(err, qt.IsNil)
}

// definerRefusalDocument is the part of a refused apply document that names
// a definer-privilege refusal.
type definerRefusalDocument struct {
	Outcome string `json:"outcome"`
	Refusal struct {
		Code              string `json:"code"`
		DefinerPrivileges []struct {
			Object    string `json:"object"`
			Definer   string `json:"definer"`
			Account   string `json:"account"`
			Privilege string `json:"privilege"`
		} `json:"definer_privileges"`
	} `json:"refusal"`
}

// savedDefinerPlan plans a changed body of my_docs as the view's definer and
// saves the plan, which names that definer.
func savedDefinerPlan(c *qt.C, target viewDefinerTarget, accounts definerAccounts) string {
	c.Helper()
	plan := filepath.Join(c.TempDir(), "change.plan.json")
	got := runPtahIn(c, "schema", "plan", "--db-url", accounts.ownerURL,
		"--schema-file", target.schemaFile(c, "SQL SECURITY DEFINER ", sqlSecurityPositive), "--output", plan)
	assertRunExited(c, got, 0)
	body, err := os.ReadFile(plan)
	c.Assert(err, qt.IsNil)
	c.Assert(string(body), qt.Contains, "DEFINER=`"+strings.TrimSuffix(accounts.owner, "@%")+"`@`%`")
	return plan
}

// TestMySQLViewDefinerSavedPlan_FailurePath applies, as a deploy account
// without the privilege, a saved plan that keeps the view's definer. The
// apply refuses before any statement with the definer-privilege code, naming
// the view, the definer, the account and the privilege, and the view is
// unchanged.
func TestMySQLViewDefinerSavedPlan_FailurePath(t *testing.T) {
	for _, engine := range sqlSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewDefinerTarget(c, engine.engine, "DEFINER", foreignOwner)
			accounts := newDefinerAccounts(c, newMySQLFamilyScratch(c, engine.engine), target.database, target.owner, "")
			plan := savedDefinerPlan(c, target, accounts)
			definition := target.definition(c)

			got := runPtahIn(c, "schema", "apply", "--db-url", accounts.deployerURL, "--plan", plan,
				"--auto-approve", "--json")

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			var document definerRefusalDocument
			c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil, qt.Commentf("%s", got.Stdout))
			c.Assert(document.Outcome, qt.Equals, "refused")
			c.Assert(document.Refusal.Code, qt.Equals, "definer-privilege")
			c.Assert(document.Refusal.DefinerPrivileges, qt.HasLen, 1)
			c.Assert(document.Refusal.DefinerPrivileges[0].Object, qt.Equals, "view my_docs")
			c.Assert(document.Refusal.DefinerPrivileges[0].Definer, qt.Equals, target.owner)
			c.Assert(document.Refusal.DefinerPrivileges[0].Account, qt.Equals, accounts.deployer)
			c.Assert(document.Refusal.DefinerPrivileges[0].Privilege, qt.Equals, definerPrivilege[engine.name])
			c.Assert(got.Stderr, qt.Contains, "GRANT "+definerPrivilege[engine.name]+" ON *.* TO '"+
				strings.TrimSuffix(accounts.deployer, "@%")+"'@'%'")
			c.Assert(target.definition(c), qt.Equals, definition)
			c.Assert(target.definer(c), qt.Equals, target.owner)
		})
	}
}

// TestMySQLViewDefinerSavedPlan_HappyPath applies the same saved plan as a
// deploy account holding the privilege, and as the definer itself, which
// needs none. The body changes and the definer stays.
func TestMySQLViewDefinerSavedPlan_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		privilege bool
		asOwner   bool
	}{
		{name: "a deploy account holding the privilege", privilege: true},
		{name: "the definer", asOwner: true},
	}
	for _, engine := range sqlSecurityEngines {
		for _, test := range tests {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				target := newViewDefinerTarget(c, engine.engine, "DEFINER", foreignOwner)
				privilege := map[bool]string{true: definerPrivilege[engine.name]}[test.privilege]
				accounts := newDefinerAccounts(c, newMySQLFamilyScratch(c, engine.engine), target.database,
					target.owner, privilege)
				plan := savedDefinerPlan(c, target, accounts)
				definition := target.definition(c)
				url := map[bool]string{true: accounts.ownerURL, false: accounts.deployerURL}[test.asOwner]

				got := runPtahIn(c, "schema", "apply", "--db-url", url, "--plan", plan, "--auto-approve", "--json")

				assertRunExited(c, got, 0)
				var document definerRefusalDocument
				c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil, qt.Commentf("%s", got.Stdout))
				c.Assert(document.Outcome, qt.Equals, "applied")
				c.Assert(target.definition(c), qt.Not(qt.Equals), definition)
				c.Assert(target.definer(c), qt.Equals, target.owner)
				c.Assert(target.held(c), qt.Equals, "DEFINER")
			})
		}
	}
}

// definerMigrationsDir writes a directory whose first file creates docs and
// whose second creates my_docs for owner, by hand, as a DBA writes it. A view
// that runs as its definer writes through that account, which the migrator
// does not run inside a file's transaction, so the file runs without one.
func definerMigrationsDir(c *qt.C, owner string) string {
	c.Helper()
	dir := c.TempDir()
	user := strings.TrimSuffix(owner, "@%")
	files := map[string]string{
		"1700000001_docs.up.sql":   "CREATE TABLE docs (id int PRIMARY KEY);\n",
		"1700000001_docs.down.sql": "DROP TABLE docs;\n",
		"1700000002_view.up.sql": "-- +ptah no_transaction\nCREATE DEFINER=`" + user + "`@`%` SQL SECURITY DEFINER " +
			"VIEW my_docs AS SELECT id FROM docs;\n",
		"1700000002_view.down.sql": "DROP VIEW my_docs;\n",
	}
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	return dir
}

// definerTables names the base tables and views of database other than the
// revision table and its run log, sorted.
func definerTables(c *qt.C, scratch mysqlScratch, database string) []string {
	c.Helper()
	rows, err := scratch.admin.QueryContext(c.Context(),
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? "+
			"AND TABLE_NAME NOT IN ('schema_migrations', 'schema_migrations_log') ORDER BY TABLE_NAME", database)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

// TestMySQLDefinerMigrationsUp_FailurePath runs a directory whose second file
// names another account's definer, as a deploy account without the
// privilege. The run refuses before the first file, so docs is not created
// and no revision is left failed: granting the privilege and running again
// applies both files with no repair, and the view keeps the definer the file
// names.
func TestMySQLDefinerMigrationsUp_FailurePath(t *testing.T) {
	for _, engine := range sqlSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			database, _ := scratch.database(c, "definer_up")
			accounts := newDefinerAccounts(c, scratch, database, "", "")
			dir := definerMigrationsDir(c, accounts.owner)

			got := runPtahIn(c, "migrations", "up", "--db-url", accounts.deployerURL, "--migrations-dir", dir)

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "view my_docs in 1700000002_view.up.sql names the definer "+accounts.owner)
			c.Assert(definerTables(c, scratch, database), qt.HasLen, 0)

			_, err := scratch.admin.ExecContext(c.Context(),
				"GRANT "+definerPrivilege[engine.name]+" ON *.* TO '"+strings.TrimSuffix(accounts.deployer, "@%")+"'@'%'")
			c.Assert(err, qt.IsNil)
			got = runPtahIn(c, "migrations", "up", "--db-url", accounts.deployerURL, "--migrations-dir", dir)

			assertRunExited(c, got, 0)
			c.Assert(definerTables(c, scratch, database), qt.DeepEquals, []string{"docs", "my_docs"})
			var definer string
			c.Assert(scratch.admin.QueryRowContext(c.Context(),
				"SELECT DEFINER FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'my_docs'",
				database).Scan(&definer), qt.IsNil)
			c.Assert(definer, qt.Equals, accounts.owner)
		})
	}
}

// TestMySQLDefinerMigrationsUp_HappyPath is the control: the account the file
// names runs it, which needs no privilege.
func TestMySQLDefinerMigrationsUp_HappyPath(t *testing.T) {
	for _, engine := range sqlSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			database, _ := scratch.database(c, "definer_own")
			accounts := newDefinerAccounts(c, scratch, database, "", "")

			got := runPtahIn(c, "migrations", "up", "--db-url", accounts.ownerURL,
				"--migrations-dir", definerMigrationsDir(c, accounts.owner))

			assertRunExited(c, got, 0)
			c.Assert(definerTables(c, scratch, database), qt.DeepEquals, []string{"docs", "my_docs"})
		})
	}
}

// replayDefinerHistory is a history whose view runs as a definer account the
// dev server holds, written by hand.
func replayDefinerHistory(user string) string {
	return "CREATE TABLE docs (id int PRIMARY KEY, owner_name varchar(64) NOT NULL);\n" +
		"CREATE DEFINER=`" + user + "`@`%` SQL SECURITY DEFINER VIEW my_docs AS " + sqlSecurityBody + ";\n"
}

// replayDefinerWarning is the start of the warning a replay-planned
// replacement of my_docs writes on stderr.
const replayDefinerWarning = "warning: view my_docs runs with its definer's privileges under SQL SECURITY DEFINER, " +
	"and the migration replaces it without a DEFINER clause"

// TestMySQLViewDefinerReplayWarnsE2E plans a changed body of a view that runs
// as its definer from a replay, with `ptah migrations generate --replay` and
// `ptah-compat migrate diff`. The replay's account and definer are the dev
// server's, so the replacement names no definer and nothing is refused. Each
// command exits 0 and warns once on stderr, and the file carries no clause.
func TestMySQLViewDefinerReplayWarnsE2E(t *testing.T) {
	for _, engine := range sqlSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			_, dev := scratch.database(c, "rdef_dev")
			user := fmt.Sprintf("ptah_rdef_%d", time.Now().UnixNano())
			_, err := scratch.admin.ExecContext(c.Context(), "CREATE USER '"+user+"'@'%'")
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dropDefinerAccount(c, scratch, user) })
			work := c.TempDir()
			desired := filepath.Join(work, "desired.sql")
			c.Assert(os.WriteFile(desired, []byte("CREATE TABLE docs (id int PRIMARY KEY, owner_name varchar(64) NOT NULL);\n"+
				"CREATE SQL SECURITY DEFINER VIEW my_docs AS "+sqlSecurityPositive+";\n"), 0o600), qt.IsNil)

			nativeDir := filepath.Join(work, "native")
			c.Assert(os.MkdirAll(nativeDir, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(nativeDir, "1700000000_init.up.sql"), []byte(replayDefinerHistory(user)), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(nativeDir, "1700000000_init.down.sql"), []byte("DROP VIEW my_docs;\nDROP TABLE docs;\n"), 0o600), qt.IsNil)

			native := runPtahIn(c, "migrations", "generate", "--replay", "--dev-url", dev,
				"--schema-file", desired, "--migrations-dir", nativeDir, "--name", "second")

			assertRunExited(c, native, 0)
			c.Assert(strings.Count(native.Stderr, replayDefinerWarning), qt.Equals, 1, qt.Commentf("%s", native.Stderr))
			c.Assert(native.Stdout, qt.Not(qt.Contains), replayDefinerWarning)
			c.Assert(definerStepBody(c, nativeDir, "_second.up.sql"), qt.Not(qt.Contains), "DEFINER=")

			compatDir := filepath.Join(work, "compat")
			c.Assert(os.MkdirAll(compatDir, 0o755), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(compatDir, "20240101000000_init.sql"), []byte(replayDefinerHistory(user)), 0o600), qt.IsNil)
			hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: work}, "migrate", "hash", "--dir", "file://"+filepath.ToSlash(compatDir))
			assertRunExited(c, hashed, 0)

			compat := clirun.Run(c, clirun.Compat, clirun.Options{Dir: work}, "migrate", "diff", "second",
				"--dir", "file://"+filepath.ToSlash(compatDir), "--to", "file://"+filepath.ToSlash(desired), "--dev-url", dev)

			assertRunExited(c, compat, 0)
			c.Assert(strings.Count(compat.Stderr, replayDefinerWarning), qt.Equals, 1, qt.Commentf("%s", compat.Stderr))
			c.Assert(compat.Stdout, qt.Not(qt.Contains), replayDefinerWarning)
			c.Assert(definerStepBody(c, compatDir, "_second.sql"), qt.Not(qt.Contains), "DEFINER=")
			c.Assert(definerStepBody(c, compatDir, "_second.sql"), qt.Contains, "CREATE OR REPLACE")
		})
	}
}

// definerStepBody reads the one file of dir whose name ends in suffix.
func definerStepBody(c *qt.C, dir, suffix string) string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	var bodies []string
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), suffix) {
			body, err := os.ReadFile(filepath.Join(dir, entry.Name()))
			c.Assert(err, qt.IsNil)
			bodies = append(bodies, string(body))
		}
	}
	c.Assert(bodies, qt.HasLen, 1)
	return bodies[0]
}

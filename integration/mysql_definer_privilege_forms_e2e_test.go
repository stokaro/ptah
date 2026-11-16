//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// The definer check reads each form a statement names a definer in, and
// refuses nothing the grants it reads cannot decide. These tests drive it
// through a saved plan's dry run, and through migration files written as a
// DBA or mysqldump writes them, naming an account, CURRENT_ROLE or a role.

// TestMySQLViewDefinerSavedPlanDryRun_FailurePath dry-runs, as a deploy
// account without the privilege, a saved plan that keeps the view's definer.
// The dry run refuses as the real run does, which the documentation states,
// and the view does not change.
func TestMySQLViewDefinerSavedPlanDryRun_FailurePath(t *testing.T) {
	for _, engine := range sqlSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			target := newViewDefinerTarget(c, engine.engine, "DEFINER", foreignOwner)
			accounts := newDefinerAccounts(c, newMySQLFamilyScratch(c, engine.engine), target.database, target.owner, "")
			plan := savedDefinerPlan(c, target, accounts)
			definition := target.definition(c)

			got := runPtahIn(c, "schema", "apply", "--db-url", accounts.deployerURL, "--plan", plan, "--dry-run")

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "view my_docs names the definer "+target.owner+
				", and the connected account "+accounts.deployer+" lacks "+definerPrivilege[engine.name])
			c.Assert(target.definition(c), qt.Equals, definition)
		})
	}
}

// definerFilesDir writes a migration directory from up files, name to body,
// each with a down file that undoes nothing.
func definerFilesDir(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := c.TempDir()
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
		down := strings.TrimSuffix(name, ".up.sql") + ".down.sql"
		c.Assert(os.WriteFile(filepath.Join(dir, down), []byte("SELECT 1;\n"), 0o600), qt.IsNil)
	}
	return dir
}

// TestMySQLDefinerDumpedMigrationsUp_FailurePath runs a directory whose second
// file names another account's definer inside the executable comments
// mysqldump writes, as a deploy account without the privilege. The run refuses
// before the first file, as for a clause written in the open.
func TestMySQLDefinerDumpedMigrationsUp_FailurePath(t *testing.T) {
	for _, engine := range sqlSecurityEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			database, _ := scratch.database(c, "definer_dump")
			accounts := newDefinerAccounts(c, scratch, database, "", "")
			owner := strings.TrimSuffix(accounts.owner, "@%")
			dir := definerFilesDir(c, map[string]string{
				"1700000001_docs.up.sql": "CREATE TABLE docs (id int PRIMARY KEY);\n",
				"1700000002_view.up.sql": "-- +ptah no_transaction\n/*!50001 CREATE ALGORITHM=UNDEFINED */\n" +
					"/*!50013 DEFINER=`" + owner + "`@`%` SQL SECURITY DEFINER */\n" +
					"/*!50001 VIEW `my_docs` AS select `id` AS `id` from `docs` */;\n",
			})

			got := runPtahIn(c, "migrations", "up", "--db-url", accounts.deployerURL, "--migrations-dir", dir)

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "view my_docs in 1700000002_view.up.sql names the definer "+accounts.owner)
			c.Assert(definerTables(c, scratch, database), qt.HasLen, 0)
		})
	}
}

// TestMySQLDefinerFormsMigrationsUp_FailurePath runs, as a deploy account
// without the privilege, a file whose view names a definer in a form MySQL
// runs: CURRENT_ROLE, which MySQL reads as the account CURRENT_ROLE@%, and a
// clause inside a comment gated on a version the server has reached. The run
// refuses before the first file.
func TestMySQLDefinerFormsMigrationsUp_FailurePath(t *testing.T) {
	// {owner} in a row stands for the user name of the scratch owner account.
	tests := []struct {
		name    string
		clause  string
		definer string
	}{
		{name: "CURRENT_ROLE", clause: "DEFINER=CURRENT_ROLE", definer: "CURRENT_ROLE@%"},
		{name: "a comment gated on MySQL 8.0.23", clause: "/*!80023 DEFINER=`{owner}`@`%` */", definer: "{owner}@%"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
			database, _ := scratch.database(c, "definer_form")
			accounts := newDefinerAccounts(c, scratch, database, "", "")
			owner := strings.TrimSuffix(accounts.owner, "@%")
			dir := definerFilesDir(c, map[string]string{
				"1700000001_docs.up.sql": "CREATE TABLE docs (id int PRIMARY KEY);\n",
				"1700000002_view.up.sql": "-- +ptah no_transaction\nCREATE " + strings.ReplaceAll(test.clause, "{owner}", owner) +
					" SQL SECURITY DEFINER VIEW my_docs AS SELECT id FROM docs;\n",
			})

			got := runPtahIn(c, "migrations", "up", "--db-url", accounts.deployerURL, "--migrations-dir", dir)

			c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, "view my_docs in 1700000002_view.up.sql names the definer "+
				strings.ReplaceAll(test.definer, "{owner}", owner))
			c.Assert(definerTables(c, scratch, database), qt.HasLen, 0)
		})
	}
}

// TestMariaDBRoleDefinerMigrationsUp_HappyPath runs, as a deploy account
// without SET USER whose default role is r, files whose views name CURRENT_ROLE
// and r as their definer. MariaDB accepts both from that account, so nothing
// is refused and the views are created.
func TestMariaDBRoleDefinerMigrationsUp_HappyPath(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin)
	// Registered before the database, so the role outlives the views it defines.
	role := fmt.Sprintf("ptah_role_%d", time.Now().UnixNano())
	c.Cleanup(func() {
		_, err := scratch.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	database, _ := scratch.database(c, "definer_role")
	accounts := newDefinerAccounts(c, scratch, database, "", "")
	deployer := strings.TrimSuffix(accounts.deployer, "@%")
	for _, statement := range []string{
		"CREATE ROLE " + role,
		"GRANT ALL ON `" + database + "`.* TO " + role,
		"GRANT " + role + " TO '" + deployer + "'@'%'",
		"SET DEFAULT ROLE " + role + " FOR '" + deployer + "'@'%'",
	} {
		_, err := scratch.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	dir := definerFilesDir(c, map[string]string{
		"1700000001_docs.up.sql": "CREATE TABLE docs (id int PRIMARY KEY);\n",
		"1700000002_views.up.sql": "-- +ptah no_transaction\n" +
			"CREATE DEFINER=CURRENT_ROLE SQL SECURITY DEFINER VIEW v_current AS SELECT id FROM docs;\n" +
			"CREATE DEFINER=`" + role + "` SQL SECURITY DEFINER VIEW v_role AS SELECT id FROM docs;\n",
	})

	got := runPtahIn(c, "migrations", "up", "--db-url", accounts.deployerURL, "--migrations-dir", dir)

	assertRunExited(c, got, 0)
	c.Assert(definerTables(c, scratch, database), qt.DeepEquals, []string{"docs", "v_current", "v_role"})
}

// TestMySQLDefinerCreatedByAnEarlierFile_HappyPath runs, as a deploy account
// holding SET_ANY_DEFINER and able to read mysql.user but without
// ALLOW_NONEXISTENT_DEFINER, a directory whose first file creates an account
// and whose second names it as a view's definer. The account does not exist
// when the run starts and does when the view is created, so nothing is
// refused and the view keeps that definer.
func TestMySQLDefinerCreatedByAnEarlierFile_HappyPath(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLFamilyScratch(c, dbtarget.MySQLAdmin)
	// Registered before the database: MySQL refuses to drop the definer of a
	// view that still exists.
	created := fmt.Sprintf("ptah_new_%d", time.Now().UnixNano())
	c.Cleanup(func() { dropDefinerAccount(c, scratch, created) })
	database, _ := scratch.database(c, "definer_new")
	accounts := newDefinerAccounts(c, scratch, database, "", "SET_ANY_DEFINER, CREATE USER")
	// #nosec G202 -- the account is the scratch deploy account this test created.
	readsAccounts := "GRANT SELECT ON mysql.user TO '" + strings.TrimSuffix(accounts.deployer, "@%") + "'@'%'"
	_, err := scratch.admin.ExecContext(c.Context(), readsAccounts)
	c.Assert(err, qt.IsNil)
	dir := definerFilesDir(c, map[string]string{
		"1700000001_account.up.sql": "-- +ptah no_transaction\nCREATE TABLE docs (id int PRIMARY KEY);\n" +
			"CREATE USER '" + created + "'@'%';\n",
		"1700000002_view.up.sql": "-- +ptah no_transaction\nCREATE DEFINER='" + created + "'@'%' SQL SECURITY DEFINER " +
			"VIEW my_docs AS SELECT id FROM docs;\n",
	})

	got := runPtahIn(c, "migrations", "up", "--db-url", accounts.deployerURL, "--migrations-dir", dir)

	assertRunExited(c, got, 0)
	var definer string
	c.Assert(scratch.admin.QueryRowContext(c.Context(),
		"SELECT DEFINER FROM information_schema.VIEWS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'my_docs'",
		database).Scan(&definer), qt.IsNil)
	c.Assert(definer, qt.Equals, created+"@%")
}

//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dbtarget"
)

// routinePrivilegeDocument is an HCL document declaring the procedure rt in
// schema with body. The procedure runs with its caller's rights, so an
// account other than its definer may replace it.
func routinePrivilegeDocument(c *qt.C, schema, body string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.hcl")
	document := fmt.Sprintf(`schema %q {}
procedure "rt" {
  schema   = schema.%s
  security = INVOKER
  as       = %q
}
`, schema, schema, body)
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}

// routinePrivilegesOn is what mysql.procs_priv holds on the routine rt of
// database, one `user@host type name privileges` per row, in order.
func routinePrivilegesOn(c *qt.C, db *sql.DB, database string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), `
		SELECT CONCAT(User, '@', Host, ' ', Routine_type, ' ', Routine_name, ' ', Proc_priv)
		FROM mysql.procs_priv
		WHERE Db = ? AND LOWER(Routine_name) = 'rt'
		ORDER BY 1`, database)
	c.Assert(err, qt.IsNil)
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	held := make([]string, 0)
	for rows.Next() {
		var row string
		c.Assert(rows.Scan(&row), qt.IsNil)
		held = append(held, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}

// routineBody is the body the server holds for the procedure rt of the
// database db is connected to.
func routineBody(c *qt.C, db *sql.DB) string {
	c.Helper()
	var body string
	c.Assert(db.QueryRowContext(c.Context(), "SELECT ROUTINE_DEFINITION FROM information_schema.ROUTINES "+
		"WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'rt'").Scan(&body), qt.IsNil)
	return body
}

// routineGrantees creates a role and a user, server-wide, that are dropped
// when the test ends, and gives them privileges on the procedure rt of
// database: the role EXECUTE and ALTER ROUTINE with the grant option, under
// the name RT, and the user, at a host pattern, EXECUTE.
func routineGrantees(c *qt.C, ctx context.Context, admin *sql.DB, database string) (role, user string) {
	c.Helper()
	suffix := time.Now().UnixNano() % 1_000_000_000
	role, user = fmt.Sprintf("rt_r_%d", suffix), fmt.Sprintf("rt_u_%d", suffix)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
		_, err = admin.ExecContext(context.Background(), "DROP USER IF EXISTS '"+user+"'@'10.9.%'")
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE ROLE " + role,
		"CREATE USER '" + user + "'@'10.9.%' IDENTIFIED BY 'Rt-pass-9'",
		"GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE `" + database + "`.RT TO " + role + " WITH GRANT OPTION",
		"GRANT EXECUTE ON PROCEDURE `" + database + "`.rt TO '" + user + "'@'10.9.%'",
	} {
		_, err := admin.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return role, user
}

// A body change to a MySQL or MariaDB procedure keeps what every grantee held
// on it. Under the default automatic_sp_privileges the DROP that replaces the
// procedure deletes its rows of mysql.procs_priv; the plan grants them again
// after the CREATE, to a role the schema does not manage and to a user at a
// host pattern, with the grant option the role held. The rows read back are
// the ones the GRANTs wrote, under the name the CREATE gives the procedure,
// and the next plan is empty. MariaDB records a role with an empty host. A URL
// naming no database reads the server, and the databases --schemas names are
// read for these rows as the one database of a URL naming it is.
func TestSchemaApplyMySQLFamilyKeepsThePrivilegesOnAReplacedRoutineLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name     string
		engine   dbtarget.Engine
		roleHost string
		scope    string
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, roleHost: "%", scope: "database"},
		{name: "mysql server", engine: dbtarget.MySQLAdmin, roleHost: "%", scope: "server"},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, roleHost: "", scope: "database"},
		{name: "mariadb server", engine: dbtarget.MariaDBAdmin, roleHost: "", scope: "server"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			url, schema, db := languageDatabase(c, ctx, test.engine)
			created, err := runPtahNativeWithError("schema", "apply", "--db-url", url,
				"--schema-file", routinePrivilegeDocument(c, schema, "SELECT 1"), "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", created))
			role, user := routineGrantees(c, ctx, db, schema)
			c.Assert(routinePrivilegesOn(c, db, schema), qt.DeepEquals, []string{
				role + "@" + test.roleHost + " PROCEDURE RT Execute,Alter Routine,Grant",
				user + "@10.9.% PROCEDURE rt Execute",
			})
			after := routinePrivilegeDocument(c, schema, "SELECT 2")
			target := scopedApply(c, url, schema, test.scope)

			replaced, err := runPtahNativeWithError(append(target, "--schema-file", after, "--auto-approve")...)
			settled, settledErr := runPtahNativeWithError(append(target, "--schema-file", after, "--dry-run")...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", replaced))
			c.Assert(replaced, qt.Contains, "DROP PROCEDURE")
			c.Assert(routineBody(c, db), qt.Equals, "SELECT 2")
			c.Assert(routinePrivilegesOn(c, db, schema), qt.DeepEquals, []string{
				role + "@" + test.roleHost + " PROCEDURE rt Execute,Alter Routine,Grant",
				user + "@10.9.% PROCEDURE rt Execute",
			})
			c.Assert(settledErr, qt.IsNil, qt.Commentf("%s", settled))
			c.Assert(settled, qt.Contains, "Schema is synced")
		})
	}
}

// scopedApply is the start of a `ptah schema apply` of url's database schema:
// through url itself for the scope "database", and through the URL of its
// server with --schemas naming schema for the scope "server".
func scopedApply(c *qt.C, url, schema, scope string) []string {
	c.Helper()
	server, err := atlasurl.WithDatabaseName(url, "")
	c.Assert(err, qt.IsNil)
	targets := map[string][]string{
		"database": {"schema", "apply", "--db-url", url},
		"server":   {"schema", "apply", "--db-url", server, "--schemas", schema},
	}
	c.Assert(targets[scope], qt.Not(qt.HasLen), 0, qt.Commentf("scope %q", scope))
	return slices.Clone(targets[scope])
}

// routineAccount creates an account, dropped when the test ends, that holds
// privileges on database, with option after the TO clause of their GRANT,
// the privilege that shows a routine's body, and the privileges extra names
// besides.
func routineAccount(
	c *qt.C, ctx context.Context, admin *sql.DB, engine dbtarget.Engine, database, privileges, option string, extra ...string,
) string {
	c.Helper()
	account := fmt.Sprintf("rt_a_%d", time.Now().UnixNano()%1_000_000_000)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP USER IF EXISTS '"+account+"'@'%'")
		c.Check(err, qt.IsNil)
	})
	routines := map[dbtarget.Engine]string{
		dbtarget.MySQLAdmin:   "GRANT SHOW_ROUTINE ON *.* TO '" + account + "'@'%'",
		dbtarget.MariaDBAdmin: "GRANT SHOW CREATE ROUTINE ON `" + database + "`.* TO '" + account + "'@'%'",
	}
	statements := []string{
		"CREATE USER '" + account + "'@'%' IDENTIFIED BY 'Rt-pass-9'",
		"GRANT " + privileges + " ON `" + database + "`.* TO '" + account + "'@'%'" + option,
		routines[engine],
	}
	for _, privilege := range extra {
		statements = append(statements, "GRANT "+privilege+" TO '"+account+"'@'%'")
	}
	for _, statement := range statements {
		_, err := admin.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	return account
}

// replacerPrivileges are what an account needs on a database to read it,
// replace its routines and grant on them, given WITH GRANT OPTION.
const replacerPrivileges = "SELECT, SHOW VIEW, TRIGGER, EXECUTE, ALTER ROUTINE, CREATE ROUTINE"

// replacedByAnAccount is a database whose procedure rt an account created
// through Ptah, holding privileges on the database with option after the TO
// clause of their GRANT, and extra, and on which [routineGrantees] gave a role
// and a user privileges. MySQL lets only an account holding SYSTEM_USER drop a
// routine whose definer holds it, so the account creates the procedure it
// replaces. It returns the account's URL, the database, a connection to it as
// the administrator and the user.
func replacedByAnAccount(
	c *qt.C, ctx context.Context, engine dbtarget.Engine, privileges, option string, extra ...string,
) (url, schema string, db *sql.DB, user string) {
	c.Helper()
	url, schema, db, _ = createdByAnAccount(c, ctx, engine, privileges, option, extra...)
	_, user = routineGrantees(c, ctx, db, schema)
	return url, schema, db, user
}

// createdByAnAccount is [replacedByAnAccount] before [routineGrantees] runs,
// with the account's name.
func createdByAnAccount(
	c *qt.C, ctx context.Context, engine dbtarget.Engine, privileges, option string, extra ...string,
) (url, schema string, db *sql.DB, account string) {
	c.Helper()
	admin, schema, db := languageDatabase(c, ctx, engine)
	account = routineAccount(c, ctx, db, engine, schema, privileges, option, extra...)
	url = replaceMySQLCredentials(c, admin, account, "Rt-pass-9")
	created, err := runPtahNativeWithError("schema", "apply", "--db-url", url,
		"--schema-file", routinePrivilegeDocument(c, schema, "SELECT 1"), "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", created))
	return url, schema, db, account
}

// withAutomaticSPPrivileges sets automatic_sp_privileges on the server db is
// connected to, and sets it back when the test ends. The variable is global,
// so a test that calls it does not run in parallel: the contour runs one
// package at a time, and a test that is not parallel runs alone in its
// package.
func withAutomaticSPPrivileges(c *qt.C, ctx context.Context, db *sql.DB, value string) {
	c.Helper()
	var was string
	c.Assert(db.QueryRowContext(ctx, "SELECT @@GLOBAL.automatic_sp_privileges").Scan(&was), qt.IsNil)
	c.Cleanup(func() {
		_, err := db.ExecContext(context.Background(), "SET GLOBAL automatic_sp_privileges = "+was)
		c.Check(err, qt.IsNil)
	})
	_, err := db.ExecContext(ctx, "SET GLOBAL automatic_sp_privileges = "+value)
	c.Assert(err, qt.IsNil)
}

// An account that may not read mysql.procs_priv cannot see who holds
// privileges on a routine, and the DROP that replaces it would take them
// without a word, so the apply is refused before anything runs: the body and
// every privilege stay. The same account created the procedure, which a
// refused read does not stand in the way of. The control grants the account
// SELECT on mysql.procs_priv, and the same apply replaces the procedure and
// gives the user its privilege back.
func TestSchemaApplyMySQLFamilyRefusesToReplaceARoutineItCannotSeeThePrivilegesOfLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			blind, schema, db, user := replacedByAnAccount(c, ctx, test.engine, replacerPrivileges, " WITH GRANT OPTION")
			after := routinePrivilegeDocument(c, schema, "SELECT 2")

			refused, err := runPtahNativeWithError("schema", "apply", "--db-url", blind, "--schema-file", after, "--auto-approve")

			c.Assert(err, qt.IsNotNil)
			c.Assert(refused, qt.Contains, "cannot replace procedure "+schema+".rt: the account may not read mysql.procs_priv")
			c.Assert(routineBody(c, db), qt.Equals, "SELECT 1")
			c.Assert(routinePrivilegesOn(c, db, schema), qt.Contains, user+"@10.9.% PROCEDURE rt Execute")
		})
		t.Run(test.name+" control", func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			seeing, schema, db, user := replacedByAnAccount(c, ctx, test.engine, replacerPrivileges, " WITH GRANT OPTION",
				"SELECT ON mysql.procs_priv")
			after := routinePrivilegeDocument(c, schema, "SELECT 2")

			replaced, err := runPtahNativeWithError("schema", "apply", "--db-url", seeing, "--schema-file", after, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", replaced))
			c.Assert(routineBody(c, db), qt.Equals, "SELECT 2")
			c.Assert(routinePrivilegesOn(c, db, schema), qt.Contains, user+"@10.9.% PROCEDURE rt Execute")
		})
	}
}

// With automatic_sp_privileges off the DROP of a routine keeps its rows of
// mysql.procs_priv and the CREATE grants none, so a body change gives nothing
// back and refuses nothing. The account holds ALL on the database without the
// grant option, so a GRANT would fail with ERROR 1370 after the DROP and the
// CREATE; with SELECT on mysql.procs_priv and without it, the apply replaces
// the procedure and every row stays as it was, under the name its GRANT
// spelled.
func TestSchemaApplyMySQLFamilyLeavesThePrivilegesADropKeepsLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
		extra  []string
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, extra: []string{"SELECT ON mysql.procs_priv"}},
		{name: "mysql without procs_priv", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, extra: []string{"SELECT ON mysql.procs_priv"}},
		{name: "mariadb without procs_priv", engine: dbtarget.MariaDBAdmin},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			url, schema, db, _ := replacedByAnAccount(c, ctx, test.engine, "ALL", "", test.extra...)
			withAutomaticSPPrivileges(c, ctx, db, "0")
			held := routinePrivilegesOn(c, db, schema)

			replaced, err := runPtahNativeWithError("schema", "apply", "--db-url", url,
				"--schema-file", routinePrivilegeDocument(c, schema, "SELECT 2"), "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", replaced))
			c.Assert(routineBody(c, db), qt.Equals, "SELECT 2")
			c.Assert(routinePrivilegesOn(c, db, schema), qt.DeepEquals, held)
			c.Assert(held, qt.HasLen, 2)
		})
	}
}

// An account holding CREATE ROUTINE on a database, and no grant option, gets
// EXECUTE and ALTER ROUTINE on each routine it creates there under the default
// automatic_sp_privileges. The CREATE that replaces the routine grants that
// row again, so the plan leaves it out rather than fail with ERROR 1370 on a
// GRANT the account may not run: the body changes, and the account holds the
// row after it.
func TestSchemaApplyMySQLFamilyLeavesTheRowTheCreateGrantsLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			url, schema, db, account := createdByAnAccount(c, ctx, test.engine, "SELECT, CREATE, CREATE ROUTINE", "",
				"SELECT ON mysql.procs_priv")
			c.Assert(routinePrivilegesOn(c, db, schema), qt.DeepEquals, []string{account + "@% PROCEDURE rt Execute,Alter Routine"})

			replaced, err := runPtahNativeWithError("schema", "apply", "--db-url", url,
				"--schema-file", routinePrivilegeDocument(c, schema, "SELECT 2"), "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", replaced))
			c.Assert(routineBody(c, db), qt.Equals, "SELECT 2")
			c.Assert(routinePrivilegesOn(c, db, schema), qt.DeepEquals, []string{account + "@% PROCEDURE rt Execute,Alter Routine"})
		})
	}
}

// The GRANTs that give the privileges back run as the applying account. One
// without the grant option on the database cannot run them, and a server
// answers ERROR 1370 after the DROP and the CREATE have run, so the plan is
// refused before anything runs: the body and every row stay. An account
// holding ALL on the database holds every privilege it would grant, and still
// not the option; one holding CREATE ROUTINE alone has its own row from the
// CREATE as well, which stays too.
func TestSchemaApplyMySQLFamilyRefusesToReplaceARoutineItMayNotGrantThePrivilegesOnLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name       string
		engine     dbtarget.Engine
		privileges string
		rows       int
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, privileges: "SELECT, CREATE, CREATE ROUTINE", rows: 3},
		{name: "mysql all", engine: dbtarget.MySQLAdmin, privileges: "ALL", rows: 2},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, privileges: "SELECT, CREATE, CREATE ROUTINE", rows: 3},
		{name: "mariadb all", engine: dbtarget.MariaDBAdmin, privileges: "ALL", rows: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			url, schema, db, user := replacedByAnAccount(c, ctx, test.engine, test.privileges, "",
				"SELECT ON mysql.procs_priv")
			held := routinePrivilegesOn(c, db, schema)

			refused, err := runPtahNativeWithError("schema", "apply", "--db-url", url,
				"--schema-file", routinePrivilegeDocument(c, schema, "SELECT 2"), "--auto-approve")

			c.Assert(err, qt.IsNotNil)
			c.Assert(refused, qt.Contains, "cannot replace procedure "+schema+".rt: the DROP that replaces it takes ")
			c.Assert(refused, qt.Contains, "and the account may not grant it again without the grant option on `"+schema+"`.*")
			c.Assert(routineBody(c, db), qt.Equals, "SELECT 1")
			c.Assert(routinePrivilegesOn(c, db, schema), qt.DeepEquals, held)
			c.Assert(held, qt.HasLen, test.rows)
			c.Assert(held, qt.Contains, user+"@10.9.% PROCEDURE rt Execute")
		})
	}
}

// A versioned migration that replaces a routine runs on a database other than
// the dev database it was planned on. The plan reads the rows it gives back on
// the dev database the directory is replayed on, and its GRANT names the
// routine as the DROP and the CREATE do, so the migration grants on the
// database it runs on: applied there, the body changes and the user holds
// EXECUTE again. A GRANT naming the dev database fails on a routine the dev
// database does not have once it is cleaned.
func TestMigrationsGenerateMySQLFamilyGivesBackThePrivilegesOnTheDatabaseItRunsOnLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			url, schema, db := languageDatabase(c, ctx, test.engine)
			dev, devName, _ := languageDatabase(c, ctx, test.engine)
			user := routineUser(c, ctx, db)
			dir := c.TempDir()
			writeRoutineMigration(c, dir, "1700000000_init.up.sql", "-- +ptah no_transaction\n"+
				"CREATE PROCEDURE rt() READS SQL DATA SQL SECURITY INVOKER SELECT 1;\n"+
				"GRANT EXECUTE ON PROCEDURE rt TO '"+user+"'@'%';\n")
			writeRoutineMigration(c, dir, "1700000000_init.down.sql", "DROP PROCEDURE rt;\n")
			desired := filepath.Join(c.TempDir(), "schema.hcl")
			c.Assert(os.WriteFile(desired, []byte("procedure \"rt\" {\n  security = INVOKER\n  as       = \"SELECT 2\"\n}\n"),
				0o600), qt.IsNil)

			generated, err := runPtahNativeWithError("migrations", "generate", "--replay", "--trust-replay-sql",
				"--dev-url", dev, "--migrations-dir", dir, "--schema-file", desired, "--name", "body")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", generated))
			applied, err := runPtahNativeWithError("migrations", "up", "--db-url", url, "--migrations-dir", dir,
				"--allow-destructive")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			c.Assert(routineMigrationNamed(c, dir, "_body.up.sql"), qt.Contains,
				"GRANT EXECUTE ON PROCEDURE `rt` TO `"+user+"`@`%`;")
			c.Assert(routineMigrationNamed(c, dir, "_body.up.sql"), qt.Not(qt.Contains), devName)
			c.Assert(routineBody(c, db), qt.Equals, "SELECT 2")
			c.Assert(routinePrivilegesOn(c, db, schema), qt.DeepEquals, []string{user + "@% PROCEDURE rt Execute"})
		})
	}
}

// routineUser creates a user at any host, dropped when the test ends.
func routineUser(c *qt.C, ctx context.Context, admin *sql.DB) string {
	c.Helper()
	user := fmt.Sprintf("rt_g_%d", time.Now().UnixNano()%1_000_000_000)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "DROP USER IF EXISTS '"+user+"'@'%'")
		c.Check(err, qt.IsNil)
	})
	_, err := admin.ExecContext(ctx, "CREATE USER '"+user+"'@'%' IDENTIFIED BY 'Rt-pass-9'")
	c.Assert(err, qt.IsNil)
	return user
}

// writeRoutineMigration writes content to the file name of dir.
func writeRoutineMigration(c *qt.C, dir, name, content string) {
	c.Helper()
	c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
}

// routineMigrationNamed is the content of the one file of dir whose name ends
// with suffix.
func routineMigrationNamed(c *qt.C, dir, suffix string) string {
	c.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*"+suffix))
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.HasLen, 1)
	content, err := os.ReadFile(matches[0])
	c.Assert(err, qt.IsNil)
	return string(content)
}

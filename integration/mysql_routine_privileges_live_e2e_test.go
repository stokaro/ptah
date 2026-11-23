//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

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
// and the next plan is empty. MariaDB records a role with an empty host.
func TestSchemaApplyMySQLFamilyKeepsThePrivilegesOnAReplacedRoutineLiveE2E(t *testing.T) {
	for _, test := range []struct {
		name     string
		engine   dbtarget.Engine
		roleHost string
	}{
		{name: "mysql", engine: dbtarget.MySQLAdmin, roleHost: "%"},
		{name: "mariadb", engine: dbtarget.MariaDBAdmin, roleHost: ""},
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

			replaced, err := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", after, "--auto-approve")
			settled, settledErr := runPtahNativeWithError("schema", "apply", "--db-url", url, "--schema-file", after, "--dry-run")

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

// replacerAccount creates an account, dropped when the test ends, that may
// read database, replace its routines and grant on them, and holds the
// privileges extra names besides.
func replacerAccount(c *qt.C, ctx context.Context, admin *sql.DB, engine dbtarget.Engine, database string, extra ...string) string {
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
		"GRANT SELECT, SHOW VIEW, TRIGGER, EXECUTE, ALTER ROUTINE, CREATE ROUTINE ON `" + database + "`.* TO '" +
			account + "'@'%' WITH GRANT OPTION",
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

// replacedByAnAccount is a database whose procedure rt an account created
// through Ptah, with the privileges [replacerAccount] gives it and extra, and
// on which [routineGrantees] gave a role and a user privileges. MySQL lets
// only an account holding SYSTEM_USER drop a routine whose definer holds it,
// so the account creates the procedure it replaces. It returns the account's
// URL, the database, a connection to it as the administrator and the user.
func replacedByAnAccount(c *qt.C, ctx context.Context, engine dbtarget.Engine, extra ...string) (url, schema string, db *sql.DB, user string) {
	c.Helper()
	admin, schema, db := languageDatabase(c, ctx, engine)
	url = replaceMySQLCredentials(c, admin, replacerAccount(c, ctx, db, engine, schema, extra...), "Rt-pass-9")
	created, err := runPtahNativeWithError("schema", "apply", "--db-url", url,
		"--schema-file", routinePrivilegeDocument(c, schema, "SELECT 1"), "--auto-approve")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", created))
	_, user = routineGrantees(c, ctx, db, schema)
	return url, schema, db, user
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
			blind, schema, db, user := replacedByAnAccount(c, ctx, test.engine)
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
			seeing, schema, db, user := replacedByAnAccount(c, ctx, test.engine, "SELECT ON mysql.procs_priv")
			after := routinePrivilegeDocument(c, schema, "SELECT 2")

			replaced, err := runPtahNativeWithError("schema", "apply", "--db-url", seeing, "--schema-file", after, "--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", replaced))
			c.Assert(routineBody(c, db), qt.Equals, "SELECT 2")
			c.Assert(routinePrivilegesOn(c, db, schema), qt.Contains, user+"@10.9.% PROCEDURE rt Execute")
		})
	}
}

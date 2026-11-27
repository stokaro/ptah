//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// TestMySQLReadRefusesARowOnlyTheAccountLookupAppliesE2E reads a database as
// a login whose database-level row applies only while no role is active. A
// name with no `%` or `_` is compared byte for byte by the lookup with an
// active role, so a row on `pb\sN` reaches the database pbsN only without
// one: measured on MySQL 8.4.13 and 26.10.0, a login holding SELECT on
// `qa\bc`.* reads qabc with SET ROLE NONE and is refused it with its role
// active. The login's default role is active on every connection, so
// information_schema lists only the table it holds a grant on, and a read that
// trusted the row would report the other table absent. The read refuses, and
// a session of the login is refused that table.
func TestMySQLReadRefusesARowOnlyTheAccountLookupAppliesE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	suffix := time.Now().UnixNano() % 1_000_000_000_000
	database := fmt.Sprintf("pbs%d", suffix)
	createMySQLDatabase(c, c.Context(), server.admin, database)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), server.admin, database) })
	role, login := granteeIdentifier(fmt.Sprintf("pbr_%d", suffix)), fmt.Sprintf("pbl_%d", suffix)
	account := granteeIdentifier(login) + "@'%'"
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account)
		c.Check(err, qt.IsNil)
		_, err = server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	partialRevokeExec(c, server, []string{
		"CREATE TABLE " + database + ".a (id int PRIMARY KEY)",
		"CREATE TABLE " + database + ".b (id int PRIMARY KEY)",
		"CREATE ROLE " + role,
		"CREATE USER " + account + " IDENTIFIED BY 'lookup_login_pw'",
		fmt.Sprintf("GRANT ALL PRIVILEGES ON `pb\\s%d`.* TO %s", suffix, account),
		"GRANT SELECT ON " + database + ".a TO " + account,
		"GRANT " + role + " TO " + account,
		"SET DEFAULT ROLE ALL TO " + account,
	})
	config := server.config.Clone()
	config.User, config.Passwd, config.DBName = login, "lookup_login_pw", ""
	loginServer := mysqlFamilyServer{admin: server.admin, config: config}

	read := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"db", "read", "--db-url", loginServer.url("mysql", database))

	c.Assert(read.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
	c.Assert(read.Stderr, qt.Contains, fmt.Sprintf("cannot see everything in database %q", database))
	session, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(session.Close(), qt.IsNil) })
	_, err = session.ExecContext(c.Context(), readProbe(database, "b"))
	c.Assert(refusedForPrivilege(err), qt.IsTrue, qt.Commentf("%v", err))
}

// TestMySQLReadTrustsARowTheAccountLookupAppliesE2E reads a database as a
// login granted a role that is not active: the session's CURRENT_ROLE() is
// NONE, so the server looks the login's database-level rows up by the account
// lookup, where a row on `pb\sN` reaches the database pbsN. The read trusts
// the row and lists both tables, and a session of the login reads the table it
// holds no table grant on.
func TestMySQLReadTrustsARowTheAccountLookupAppliesE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	suffix := time.Now().UnixNano() % 1_000_000_000_000
	database := fmt.Sprintf("pbs%d", suffix)
	createMySQLDatabase(c, c.Context(), server.admin, database)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), server.admin, database) })
	role, login := granteeIdentifier(fmt.Sprintf("pbr_%d", suffix)), fmt.Sprintf("pbl_%d", suffix)
	account := granteeIdentifier(login) + "@'%'"
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account)
		c.Check(err, qt.IsNil)
		_, err = server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	partialRevokeExec(c, server, []string{
		"CREATE TABLE " + database + ".a (id int PRIMARY KEY)",
		"CREATE TABLE " + database + ".b (id int PRIMARY KEY)",
		"CREATE ROLE " + role,
		"CREATE USER " + account + " IDENTIFIED BY 'lookup_login_pw'",
		fmt.Sprintf("GRANT ALL PRIVILEGES ON `pb\\s%d`.* TO %s", suffix, account),
		"GRANT SHOW_ROUTINE ON *.* TO " + account,
		"GRANT SELECT ON " + database + ".a TO " + account,
		"GRANT " + role + " TO " + account,
	})
	config := server.config.Clone()
	config.User, config.Passwd, config.DBName = login, "lookup_login_pw", ""
	loginServer := mysqlFamilyServer{admin: server.admin, config: config}

	read := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"db", "read", "--db-url", loginServer.url("mysql", database))

	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
	c.Assert(read.Stdout, qt.Contains, "CREATE TABLE `b`")
	session, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(session.Close(), qt.IsNil) })
	_, err = session.ExecContext(c.Context(), readProbe(database, "b"))
	c.Assert(err, qt.IsNil)
}

// TestMySQLReadTrustsAnActiveRoleRowTheServerFoldsE2E reads a database as a
// login whose default role, active on every connection, holds the database
// through a row spelled in upper case, on a server started with
// --lower-case-table-names=1. The server folds the row's name for the lookup
// with an active role, so the role reaches the database: the read lists the
// table, and a session of the login reads it.
func TestMySQLReadTrustsAnActiveRoleRowTheServerFoldsE2E(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLFoldedServer)
	suffix := time.Now().UnixNano() % 1_000_000_000_000
	database := fmt.Sprintf("rxy%d", suffix)
	createMySQLDatabase(c, c.Context(), server.admin, database)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), server.admin, database) })
	role, login := granteeIdentifier(fmt.Sprintf("rxr_%d", suffix)), fmt.Sprintf("rxu_%d", suffix)
	account := granteeIdentifier(login) + "@'%'"
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account)
		c.Check(err, qt.IsNil)
		_, err = server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+role)
		c.Check(err, qt.IsNil)
	})
	partialRevokeExec(c, server, []string{
		"CREATE TABLE " + database + ".t1 (id int PRIMARY KEY)",
		"CREATE ROLE " + role,
		"CREATE USER " + account + " IDENTIFIED BY 'lookup_login_pw'",
		fmt.Sprintf("GRANT SELECT, SHOW VIEW, TRIGGER ON `RXY%d`.* TO %s", suffix, role),
		"GRANT SHOW_ROUTINE ON *.* TO " + account,
		"GRANT " + role + " TO " + account,
		"SET DEFAULT ROLE ALL TO " + account,
	})
	config := server.config.Clone()
	config.User, config.Passwd, config.DBName = login, "lookup_login_pw", ""
	loginServer := mysqlFamilyServer{admin: server.admin, config: config}

	read := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: time.Minute},
		"db", "read", "--db-url", loginServer.url("mysql", database))

	c.Assert(read.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", read.Stdout, read.Stderr))
	c.Assert(read.Stdout, qt.Contains, "CREATE TABLE `t1`")
	session, err := sql.Open("mysql", config.FormatDSN())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(session.Close(), qt.IsNil) })
	_, err = session.ExecContext(c.Context(), readProbe(database, "t1"))
	c.Assert(err, qt.IsNil)
}

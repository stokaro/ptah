//go:build integration

package passworddisplay_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the mysql driver the account is created through

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// newPostgreSQLAccount creates a role with the password and a database it owns,
// and returns the address that reaches them. PostgreSQL's drivers read a URL, so
// the password is escaped in it: what a run can leak here is the decoded value.
func newPostgreSQLAccount(c *qt.C) string {
	c.Helper()
	adminAddress := dbtarget.URL(c, dbtarget.PostgreSQL)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminAddress)
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_password_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+name+" LOGIN PASSWORD '"+password+"'")
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name+" OWNER "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		c.Check(dropErr, qt.IsNil)
		_, dropErr = admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+name)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	parsed, err := url.Parse(adminAddress)
	c.Assert(err, qt.IsNil)
	parsed.User = url.UserPassword(name, password)
	parsed.Path = "/" + name
	return parsed.String()
}

// newMySQLFamilyAccount creates an account with the password and a database it
// may write, and returns the two forms of address that reach them: the driver's
// own, with the password as it is, and a URL, with the password escaped.
func newMySQLFamilyAccount(c *qt.C, engine dbtarget.Engine) (driverForm, urlForm string) {
	c.Helper()
	adminAddress := dbtarget.URL(c, engine)
	server, err := atlasurl.ParseMySQLURL(adminAddress)
	c.Assert(err, qt.IsNil)
	admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	name := fmt.Sprintf("ptah_password_%d", time.Now().UnixNano()%1_000_000_000)
	account := "'" + name + "'@'%'"
	for _, statement := range []string{
		"CREATE DATABASE `" + name + "`",
		"CREATE USER " + account + " IDENTIFIED BY '" + password + "'",
		"GRANT ALL ON `" + name + "`.* TO " + account,
	} {
		_, err = admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS `"+name+"`")
		c.Check(dropErr, qt.IsNil)
		_, dropErr = admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+account)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})
	host, port, ok := server.HostPort()
	c.Assert(ok, qt.IsTrue)
	driverForm = server.Scheme() + "://" + name + ":" + password + "@tcp(" + host + ":" + port + ")/" + name
	urlForm = server.Scheme() + "://" + name + ":" + url.QueryEscape(password) + "@" + host + ":" + port + "/" + name
	return driverForm, urlForm
}

// successfulRuns are the verbs that report on a database they reached, in each
// output they have.
var successfulRuns = []struct {
	name   string
	target clirun.Target
	args   []string
}{
	{name: "migrations up, JSON", target: clirun.Ptah, args: []string{"migrations", "up", "--db-url", "ADDRESS", "--migrations-dir", "native", "--json"}},
	{name: "migrations status, JSON", target: clirun.Ptah, args: []string{"migrations", "status", "--db-url", "ADDRESS", "--migrations-dir", "native", "--json"}},
	{name: "migrations log", target: clirun.Ptah, args: []string{"migrations", "log", "--db-url", "ADDRESS"}},
	{name: "migrations log, JSON", target: clirun.Ptah, args: []string{"migrations", "log", "--db-url", "ADDRESS", "--json"}},
	{name: "migrations plan", target: clirun.Ptah, args: []string{"migrations", "plan", "--db-url", "ADDRESS", "--root-dir", "models"}},
	{name: "migrations generate", target: clirun.Ptah, args: []string{"migrations", "generate", "--db-url", "ADDRESS", "--root-dir", "models", "--migrations-dir", "generated"}},
	{name: "migrations down", target: clirun.Ptah, args: []string{"migrations", "down", "--db-url", "ADDRESS", "--migrations-dir", "native", "--target", "0", "--confirm"}},
	{name: "migrations up, verbose", target: clirun.Ptah, args: []string{"migrations", "up", "--db-url", "ADDRESS", "--migrations-dir", "native", "--verbose"}},
	{name: "db read", target: clirun.Ptah, args: []string{"db", "read", "--db-url", "ADDRESS"}},
	{name: "db capabilities, JSON", target: clirun.Ptah, args: []string{"db", "capabilities", "--db-url", "ADDRESS", "--format", "json"}},
	{name: "db verify", target: clirun.Ptah, args: []string{"db", "verify", "--db-url", "ADDRESS", "--checks", "checks"}},
	{name: "schema inspect", target: clirun.Ptah, args: []string{"schema", "inspect", "--db-url", "ADDRESS"}},
	{name: "schema compare", target: clirun.Ptah, args: []string{"schema", "compare", "--db-url", "ADDRESS", "--root-dir", "models"}},
	{name: "schema plan, to a file", target: clirun.Ptah, args: []string{"schema", "plan", "--db-url", "ADDRESS", "--root-dir", "models", "--output", "plan.json"}},
	{name: "schema plan, JSON", target: clirun.Ptah, args: []string{"schema", "plan", "--db-url", "ADDRESS", "--root-dir", "models", "--dry-run", "--json"}},
	{name: "compat migrate status", target: clirun.Compat, args: []string{"migrate", "status", "--url", "ADDRESS", "--dir", "file://migrations"}},
	{name: "compat migrate status, JSON", target: clirun.Compat, args: []string{"migrate", "status", "--url", "ADDRESS", "--dir", "file://migrations", "--format", "{{ json . }}"}},
	{name: "compat schema inspect", target: clirun.Compat, args: []string{"schema", "inspect", "--url", "ADDRESS"}},
}

// assertRunsPrintNoPassword runs every verb against a database it reaches, and
// looks for the password in what it printed, in the files it wrote and in the
// rows of the revision and log tables it wrote.
func assertRunsPrintNoPassword(c *qt.C, address, wantDisplay string) {
	c.Helper()
	dir := newWorkspace(c)
	for _, run := range successfulRuns {
		got := clirun.Run(c, run.target, clirun.Options{Dir: dir}, withAddress(run.args, address)...)
		c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%s\nstdout:\n%s\nstderr:\n%s", run.name, got.Stdout, got.Stderr))
		assertNoPassword(c, got.Stdout)
		assertNoPassword(c, got.Stderr)
	}
	assertNoPasswordInFiles(c, dir)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir}, "db", "read", "--db-url", address)
	c.Assert(got.Stderr, qt.Contains, "Reading schema from database: "+wantDisplay)

	rows := readRows(c, address, "schema_migrations") + readRows(c, address, "schema_migrations_log")
	c.Assert(rows, qt.Contains, fixtureVersion)
	assertNoPassword(c, rows)
}

// readRows is every row of table as text.
func readRows(c *qt.C, address, table string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), "SELECT * FROM "+table)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	columns, err := rows.Columns()
	c.Assert(err, qt.IsNil)
	values := make([]sql.NullString, len(columns))
	targets := make([]any, len(columns))
	for i := range values {
		targets[i] = &values[i]
	}
	var text strings.Builder
	for rows.Next() {
		c.Assert(rows.Scan(targets...), qt.IsNil)
		for _, value := range values {
			text.WriteString(value.String + "\n")
		}
	}
	c.Assert(rows.Err(), qt.IsNil)
	return text.String()
}

func TestAPostgreSQLRunPrintsNoPasswordE2E(t *testing.T) {
	c := qt.New(t)
	address := newPostgreSQLAccount(c)
	parsed, err := url.Parse(address)
	c.Assert(err, qt.IsNil)
	want := "postgres://" + parsed.User.Username() + ":***@" + parsed.Host + parsed.Path

	assertRunsPrintNoPassword(c, address, want)
}

func TestAMySQLFamilyRunPrintsNoPasswordE2E(t *testing.T) {
	for _, engine := range []dbtarget.Engine{dbtarget.MySQLAdmin, dbtarget.MariaDBAdmin} {
		t.Run(engine.String(), func(t *testing.T) {
			c := qt.New(t)
			driverForm, urlForm := newMySQLFamilyAccount(c, engine)
			parsed, err := atlasurl.ParseMySQLURL(urlForm)
			c.Assert(err, qt.IsNil)

			assertRunsPrintNoPassword(c, driverForm, parsed.Scheme()+"://"+parsed.User()+":***@tcp("+parsed.Address()+")/"+parsed.Database())
			assertRunsPrintNoPassword(c, urlForm, parsed.Scheme()+"://"+parsed.User()+":***@"+parsed.Address()+"/"+parsed.Database())
		})
	}
}

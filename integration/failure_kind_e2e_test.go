//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql" // registers the mysql driver for database/sql
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/core/platform"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/errorkind"
	"ptah.run/migration/safety"
)

// The kind each command names when it cannot open its target, measured
// through the shipped binary against the live servers CI starts.
//
// The kind is decided from the driver's typed error, so these rows are what
// shows the drivers return those types against a real server: a fake one in
// internal/errorkind speaks just enough of each protocol to produce them, and
// cannot show that PostgreSQL answers an unknown database with 3D000 or that
// MySQL's caching_sha2_password exchange ends in 1045. Each row runs every
// command the Kubernetes operator runs with a machine-readable document,
// because each opens its target through its own call site and marks the
// failure there.
//
// The TLS rows are the ones the CI servers can stage. MySQL generates a
// certificate no client trusts, so a URL that requires a verified one fails
// its handshake. The PostgreSQL server runs without TLS, and pgx reports a
// server that refuses TLS as text with no type, which leaves that failure
// without a kind; internal/errorkind covers PostgreSQL's certificate failures
// with its own server.

// failureKindCommand is one command a row runs, with everything but the
// database URL.
type failureKindCommand struct {
	name    string
	command string
	before  []string
	after   []string
}

// failureKindDocument is the part of each command's document these rows read.
type failureKindDocument struct {
	Command string            `json:"command"`
	Error   *errorkind.Report `json:"error"`
}

// failureKindCase is a target URL and the kind its failure carries.
type failureKindCase struct {
	name string
	url  string
	want errorkind.Kind
}

// failureKindCommands are the commands every row runs, against a directory
// holding a desired schema, a migration and a plan for dialect.
func failureKindCommands(c *qt.C, dialect string) []failureKindCommand {
	c.Helper()
	dir := c.TempDir()
	schemaFile := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(schemaFile, []byte("CREATE TABLE failure_kind_probe (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "0000000001_probe.up.sql"),
		[]byte("CREATE TABLE failure_kind_probe (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(migrations, "0000000001_probe.down.sql"),
		[]byte("DROP TABLE failure_kind_probe;\n"), 0o600), qt.IsNil)
	plan, err := atlasschema.MarshalPlanFile(atlasschema.PlanFile{
		FormatVersion:   atlasschema.PlanFormatVersion,
		Name:            "failure_kind_probe",
		Dialect:         dialect,
		FromFingerprint: "sha256:" + fmt.Sprintf("%064x", 1),
		ToFingerprint:   "sha256:" + fmt.Sprintf("%064x", 2),
		Statements: []atlasschema.PlanStatement{{
			SQL: "CREATE TABLE failure_kind_probe (id INTEGER PRIMARY KEY)", Severity: safety.Safe, Reason: "create table",
		}},
	})
	c.Assert(err, qt.IsNil)
	planFile := filepath.Join(dir, "probe.plan.json")
	c.Assert(os.WriteFile(planFile, plan, 0o600), qt.IsNil)

	return []failureKindCommand{
		{name: "schema drift", command: "schema drift", before: []string{"schema", "drift"},
			after: []string{"--schema-file", schemaFile, "--format", "json"}},
		{name: "schema plan", command: "schema plan", before: []string{"schema", "plan"},
			after: []string{"--schema-file", schemaFile, "--output", filepath.Join(dir, "new.plan.json"), "--json"}},
		{name: "schema apply", command: "schema apply", before: []string{"schema", "apply"},
			after: []string{"--plan", planFile, "--auto-approve", "--json"}},
		{name: "schema apply --dry-run", command: "schema apply", before: []string{"schema", "apply"},
			after: []string{"--plan", planFile, "--dry-run", "--json"}},
		{name: "migrations status", command: "migrations status", before: []string{"migrations", "status"},
			after: []string{"--migrations-dir", migrations, "--json"}},
		{name: "migrations up", command: "migrations up", before: []string{"migrations", "up"},
			after: []string{"--migrations-dir", migrations, "--json"}},
	}
}

// runFailureKindCases runs every command against every case and holds each
// document to the case's kind.
func runFailureKindCases(t *testing.T, commands []failureKindCommand, cases []failureKindCase) {
	t.Helper()
	for _, failure := range cases {
		t.Run(failure.name, func(t *testing.T) {
			for _, command := range commands {
				t.Run(command.name, func(t *testing.T) {
					c := qt.New(t)
					args := slices.Concat(command.before, []string{"--db-url", failure.url}, command.after)

					got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir(), Timeout: 2 * time.Minute}, args...)

					c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					var document failureKindDocument
					c.Assert(json.Unmarshal([]byte(got.Stdout), &document), qt.IsNil, qt.Commentf("stdout:\n%s", got.Stdout))
					c.Assert(document.Command, qt.Equals, command.command)
					c.Assert(document.Error, qt.IsNotNil)
					c.Assert(document.Error.Kind, qt.Equals, failure.want, qt.Commentf("%s", document.Error.Message))
					c.Assert(got.Stderr, qt.Contains, "error: "+document.Error.Message+"\n")
				})
			}
		})
	}
}

// failureKindRefusedAddress is a loopback host:port nothing listens on.
func failureKindRefusedAddress(c *qt.C) string {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	address := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)
	return address
}

// failureKindUnresolvableHost never resolves: RFC 6761 reserves .invalid.
const failureKindUnresolvableHost = "ptah-failure-kind.invalid"

// TestFailureKindPostgreSQLE2E names each way a PostgreSQL target fails to
// open.
func TestFailureKindPostgreSQLE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	target, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	user := target.User.Username()
	with := func(edit func(*url.URL)) string {
		copied := *target
		edit(&copied)
		return copied.String()
	}

	// A role the server knows, with the right password, on a database whose
	// CONNECT privilege PUBLIC no longer holds.
	admin, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = admin.Close() })
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	fencedDatabase, fencedRole := "ptah_failure_kind_"+suffix, "ptah_failure_kind_"+suffix
	createE2EDatabase(c, ctx, admin, fencedDatabase)
	t.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, fencedDatabase) })
	_, err = admin.ExecContext(ctx, "REVOKE CONNECT ON DATABASE "+quoteE2EIdent(fencedDatabase)+" FROM PUBLIC")
	c.Assert(err, qt.IsNil)
	_, err = admin.ExecContext(ctx, "CREATE ROLE "+quoteE2EIdent(fencedRole)+" LOGIN PASSWORD 'fenced-password'")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+quoteE2EIdent(fencedRole))
		c.Assert(dropErr, qt.IsNil)
	})

	runFailureKindCases(t, failureKindCommands(c, platform.Postgres), []failureKindCase{
		{name: "the host name does not resolve", want: errorkind.DatabaseUnreachable,
			url: with(func(u *url.URL) { u.Host = failureKindUnresolvableHost + ":" + target.Port() })},
		{name: "nothing listens on the port", want: errorkind.DatabaseUnreachable,
			url: with(func(u *url.URL) { u.Host = failureKindRefusedAddress(c) })},
		{name: "the password is wrong", want: errorkind.AuthenticationFailed,
			url: with(func(u *url.URL) { u.User = url.UserPassword(user, "wrong-password") })},
		{name: "the role does not exist", want: errorkind.AuthenticationFailed,
			url: with(func(u *url.URL) { u.User = url.UserPassword("ptah_failure_kind_nobody", "password") })},
		{name: "the database does not exist", want: errorkind.DatabaseNotFound,
			url: with(func(u *url.URL) { u.Path = "/ptah_failure_kind_missing" })},
		{name: "the role may not connect to the database", want: errorkind.PermissionDenied,
			url: with(func(u *url.URL) {
				u.User = url.UserPassword(fencedRole, "fenced-password")
				u.Path = "/" + fencedDatabase
			})},
	})
}

// TestFailureKindMySQLE2E names each way a MySQL target fails to open.
//
// The missing database connects as the administrative account. MySQL answers
// an account that holds no privilege on a database name with access denied
// (1044) whether or not the database exists, so only an account with global
// privileges hears that it does not (1049).
func TestFailureKindMySQLE2E(t *testing.T) {
	c := qt.New(t)
	ordinary := mysqlFailureKindParts(c, dbtarget.MySQL)
	privileged := mysqlFailureKindParts(c, dbtarget.MySQLAdmin)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)

	// An account the server knows, with the right password, and no privilege
	// on the database it names.
	admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, dbtarget.MySQLAdmin))
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { _ = admin.Close() })
	fencedUser := fmt.Sprintf("ptah_fk_%d", time.Now().UnixNano()%1_000_000_000)
	_, err = admin.ExecContext(ctx, "CREATE USER '"+fencedUser+"'@'%' IDENTIFIED BY 'fenced-password'")
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP USER IF EXISTS '"+fencedUser+"'@'%'")
		c.Assert(dropErr, qt.IsNil)
	})

	runFailureKindCases(t, failureKindCommands(c, platform.MySQL), []failureKindCase{
		{name: "the host name does not resolve", want: errorkind.DatabaseUnreachable,
			url: ordinary.url(ordinary.user, ordinary.password, failureKindUnresolvableHost+":"+ordinary.port, ordinary.database, "")},
		{name: "nothing listens on the port", want: errorkind.DatabaseUnreachable,
			url: ordinary.url(ordinary.user, ordinary.password, failureKindRefusedAddress(c), ordinary.database, "")},
		{name: "the password is wrong", want: errorkind.AuthenticationFailed,
			url: ordinary.url(ordinary.user, "wrong-password", ordinary.address, ordinary.database, "")},
		{name: "the database does not exist", want: errorkind.DatabaseNotFound,
			url: privileged.url(privileged.user, privileged.password, privileged.address, "ptah_failure_kind_missing", "")},
		{name: "the account may not use the database", want: errorkind.PermissionDenied,
			url: ordinary.url(fencedUser, "fenced-password", ordinary.address, ordinary.database, "")},
		{name: "the certificate does not verify", want: errorkind.TLSFailed,
			url: ordinary.url(ordinary.user, ordinary.password, ordinary.address, ordinary.database, "tls=true")},
	})
}

// mysqlFailureKindTarget is a MySQL address taken apart, so a row can change
// one part of it.
type mysqlFailureKindTarget struct {
	user, password, address, port, database string
}

func mysqlFailureKindParts(c *qt.C, engine dbtarget.Engine) mysqlFailureKindTarget {
	c.Helper()
	parsed, err := atlasurl.ParseMySQLURL(dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	_, port, ok := parsed.HostPort()
	c.Assert(ok, qt.IsTrue, qt.Commentf("the %s target is not a TCP address", engine))
	return mysqlFailureKindTarget{
		user: parsed.User(), password: parsed.Password(), address: parsed.Address(), port: port, database: parsed.Database(),
	}
}

// url spells a MySQL URL in the form the commands read.
func (mysqlFailureKindTarget) url(user, password, address, database, query string) string {
	built := url.URL{Scheme: "mysql", User: url.UserPassword(user, password), Host: address, Path: "/" + database, RawQuery: query}
	return built.String()
}

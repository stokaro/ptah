//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// mysqlSocketServers are the servers these tests reach through a Unix socket,
// each with the socket spelling it is reached by.
//
// The pinned community binary v1.3.0 opens `mysql+unix`, `mariadb+unix` and
// `maria+unix` on every verb that takes a database URL, as `--url` and as
// `--dev-url`. In that form the path is the socket and the `database`
// parameter names the database, and the binary's `schema inspect` output
// through the socket is byte-identical to its output over TCP, measured
// against MariaDB 11.8.9 and MySQL 8.4.11 (stokaro/ptah#3755).
//
// Each server is also reached over TCP with an administrative account. That
// connection creates the scratch databases and reads the catalog back, so what
// a test asserts never comes through the socket URL it is testing.
var mysqlSocketServers = []struct {
	name   string
	socket dbtarget.Engine
	admin  dbtarget.Engine
	scheme string
}{
	{name: "mysql+unix to MySQL", socket: dbtarget.MySQLSocket, admin: dbtarget.MySQLAdmin, scheme: "mysql+unix"},
	{name: "mariadb+unix to MariaDB", socket: dbtarget.MariaDBSocket, admin: dbtarget.MariaDBAdmin, scheme: "mariadb+unix"},
	{name: "maria+unix to MariaDB", socket: dbtarget.MariaDBSocket, admin: dbtarget.MariaDBAdmin, scheme: "maria+unix"},
}

// mysqlSocketTable is the desired state the tests apply.
const mysqlSocketTable = "CREATE TABLE widgets (id int NOT NULL, name varchar(20) NOT NULL, PRIMARY KEY (id));"

// mysqlSocketScratch is one server, reached through its socket and over TCP.
type mysqlSocketScratch struct {
	admin    *sql.DB
	adminURL string
	socket   *url.URL
}

func newMySQLSocketScratch(c *qt.C, socketEngine, adminEngine dbtarget.Engine, scheme string) mysqlSocketScratch {
	c.Helper()
	admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, adminEngine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	c.Assert(admin.PingContext(c.Context()), qt.IsNil)

	// Read with net/url rather than with the parser under test, so the URL a
	// test hands Ptah is built by something other than what reads it.
	socket, err := url.Parse(dbtarget.URL(c, socketEngine))
	c.Assert(err, qt.IsNil)
	socket.Scheme = scheme
	return mysqlSocketScratch{admin: admin, adminURL: dbtarget.URL(c, adminEngine), socket: socket}
}

// database creates an empty database, dropped when the test ends.
func (s mysqlSocketScratch) database(c *qt.C, prefix string) string {
	c.Helper()
	name := fmt.Sprintf("ptah_%s_%d", prefix, time.Now().UnixNano())
	createMySQLDatabase(c, c.Context(), s.admin, name)
	c.Cleanup(func() { dropMySQLDatabase(c, context.Background(), s.admin, name) })
	return name
}

// socketURL is the socket URL naming the database.
func (s mysqlSocketScratch) socketURL(database string) string {
	named := *s.socket
	query := named.Query()
	query.Set("database", database)
	named.RawQuery = query.Encode()
	return named.String()
}

// tcpURL is the TCP URL of the same server naming the database.
func (s mysqlSocketScratch) tcpURL(c *qt.C, database string) string {
	c.Helper()
	return replaceMySQLDatabaseName(c, s.adminURL, database)
}

// tables answers the tables of a database, in name order, read from the
// catalog over TCP.
func (s mysqlSocketScratch) tables(c *qt.C, database string) string {
	c.Helper()
	var tables sql.NullString
	err := s.admin.QueryRowContext(c.Context(), `
		SELECT GROUP_CONCAT(TABLE_NAME ORDER BY TABLE_NAME)
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?`,
		database,
	).Scan(&tables)
	c.Assert(err, qt.IsNil)
	return tables.String
}

// writeMySQLSocketSchema writes the desired state and returns its path.
func writeMySQLSocketSchema(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(mysqlSocketTable+"\n"), 0o600), qt.IsNil)
	return path
}

// TestCompatSchemaApplyConnectsThroughASocketE2E applies a schema file to a
// database named by a socket URL, rehearsing on a dev database named the same
// way, and reads the table back from the catalog.
func TestCompatSchemaApplyConnectsThroughASocketE2E(t *testing.T) {
	for _, server := range mysqlSocketServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLSocketScratch(c, server.socket, server.admin, server.scheme)
			target := scratch.database(c, "sock_target")
			dev := scratch.database(c, "sock_dev")

			out, err := runCompatVerb("schema", "apply",
				"--url", scratch.socketURL(target),
				"--dev-url", scratch.socketURL(dev),
				"--to", "file://"+writeMySQLSocketSchema(c),
				"--auto-approve")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(scratch.tables(c, target), qt.Equals, "widgets")
			c.Assert(scratch.tables(c, dev), qt.Equals, "")
		})
	}
}

// TestCompatSchemaInspectReadsASocketAsItsTCPAddressE2E inspects one database
// through the socket and over TCP: the output is the same document, which is
// how the community binary answered the same comparison.
func TestCompatSchemaInspectReadsASocketAsItsTCPAddressE2E(t *testing.T) {
	for _, server := range mysqlSocketServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLSocketScratch(c, server.socket, server.admin, server.scheme)
			target := scratch.database(c, "sock_inspect")
			_, err := scratch.admin.ExecContext(c.Context(), "CREATE TABLE `"+target+"`.widgets (id int NOT NULL, PRIMARY KEY (id))")
			c.Assert(err, qt.IsNil)

			overTCP, err := runCompatVerb("schema", "inspect", "--url", scratch.tcpURL(c, target))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", overTCP))
			throughSocket, err := runCompatVerb("schema", "inspect", "--url", scratch.socketURL(target))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", throughSocket))
			c.Assert(throughSocket, qt.Contains, `schema "`+target+`"`)
			c.Assert(throughSocket, qt.Equals, overTCP)
		})
	}
}

// TestCompatMigrateVerbsConnectThroughASocketE2E plans a migration on a dev
// database, applies it and reports its status, each through the socket, and
// reads the table back from the catalog.
func TestCompatMigrateVerbsConnectThroughASocketE2E(t *testing.T) {
	for _, server := range mysqlSocketServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLSocketScratch(c, server.socket, server.admin, server.scheme)
			target := scratch.database(c, "sock_migrate")
			dev := scratch.database(c, "sock_migrate_dev")
			dir := filepath.Join(c.TempDir(), "migrations")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)

			planned, err := runCompatVerb("migrate", "diff", "init",
				"--dir", "file://"+dir, "--to", "file://"+writeMySQLSocketSchema(c), "--dev-url", scratch.socketURL(dev))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", planned))
			applied, err := runCompatVerb("migrate", "apply", "--dir", "file://"+dir, "--url", scratch.socketURL(target))
			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			status, err := runCompatVerb("migrate", "status", "--dir", "file://"+dir, "--url", scratch.socketURL(target))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", status))
			c.Assert(status, qt.Contains, "Migration Status: OK")
			c.Assert(scratch.tables(c, target), qt.Equals, "atlas_schema_revisions,widgets")
		})
	}
}

// TestCompatRefusesADevURLNamingTheTargetThroughAnotherTransportE2E points the
// dev URL at the target database over TCP while the target is named through
// the socket. The dev database is reset before the plan is rehearsed on it, so
// the run is refused and the target keeps its table.
//
// Read from the path, the target is a database called after the socket, the two
// URLs look distinct, and the reset runs against the target. The community
// binary refuses the same argv, because the dev database it is given is not
// clean.
func TestCompatRefusesADevURLNamingTheTargetThroughAnotherTransportE2E(t *testing.T) {
	for _, server := range mysqlSocketServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLSocketScratch(c, server.socket, server.admin, server.scheme)
			target := scratch.database(c, "sock_guard")
			_, err := scratch.admin.ExecContext(c.Context(), "CREATE TABLE `"+target+"`.keep (id int NOT NULL, PRIMARY KEY (id))")
			c.Assert(err, qt.IsNil)

			out, err := runCompatVerb("schema", "apply",
				"--url", scratch.socketURL(target),
				"--dev-url", scratch.tcpURL(c, target),
				"--to", "file://"+writeMySQLSocketSchema(c),
				"--auto-approve")

			c.Assert(err, qt.ErrorMatches, `--dev-url must not point at the target database: .*`, qt.Commentf("%s", out))
			c.Assert(scratch.tables(c, target), qt.Equals, "keep")
		})
	}
}

// TestNativeSchemaApplyConnectsThroughASocketE2E is the native half. The socket
// spellings are transports of a dialect Ptah already reads, declared where every
// surface reads a dialect, so `ptah` connects through them as `ptah-compat`
// does.
func TestNativeSchemaApplyConnectsThroughASocketE2E(t *testing.T) {
	for _, server := range mysqlSocketServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLSocketScratch(c, server.socket, server.admin, server.scheme)
			target := scratch.database(c, "sock_native")
			dev := scratch.database(c, "sock_native_dev")

			runPtahNative(c, "schema", "apply",
				"--db-url", scratch.socketURL(target),
				"--dev-url", scratch.socketURL(dev),
				"--schema-file", writeMySQLSocketSchema(c),
				"--auto-approve")

			c.Assert(scratch.tables(c, target), qt.Equals, "widgets")
		})
	}
}

// TestCompatRefusesASocketURLNamingNoDatabaseE2E connects through the socket
// with no database. The community binary reads that URL as the whole server,
// which Ptah does not (stokaro/ptah#3761), so the refusal names the missing
// database rather than failing on the NULL the server answers for it.
func TestCompatRefusesASocketURLNamingNoDatabaseE2E(t *testing.T) {
	for _, server := range mysqlSocketServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLSocketScratch(c, server.socket, server.admin, server.scheme)

			out, err := runCompatVerb("schema", "inspect", "--url", scratch.socket.String())

			c.Assert(err, qt.ErrorMatches, `(?s).*the database URL names no database.*`, qt.Commentf("%s", out))
		})
	}
}

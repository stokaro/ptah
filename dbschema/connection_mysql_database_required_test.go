package dbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// mysqlURLNamesNoDatabase is the refusal of a MySQL-family URL that names no
// database. It says what to write instead, because the pinned community binary
// reads such a URL as the whole server and an operator coming from it has
// nothing else to go on (stokaro/ptah#3761).
const mysqlURLNamesNoDatabase = `the database URL names no database; Ptah reads and changes one MySQL or MariaDB database per run, not a whole server\. ` +
	`Name the database in the URL path \(mysql://user@host:3306/app\) or, for a socket URL, in the database parameter ` +
	`\(mysql\+unix://user@/run/mysqld/mysqld\.sock\?database=app\), and run the command once for each database`

// TestConnectToDatabase_RefusesAMySQLURLThatNamesNoDatabase refuses every
// form of such a URL before anything is dialed. Each points at a port or a
// socket nothing listens on, so a refusal that came after the dial would be a
// connection error instead.
func TestConnectToDatabase_RefusesAMySQLURLThatNamesNoDatabase(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "URL form", url: "mysql://root@127.0.0.1:1"},
		{name: "URL form ending in a slash", url: "mariadb://root@127.0.0.1:1/"},
		{name: "driver TCP form", url: "maria://root@tcp(127.0.0.1:1)/"},
		{name: "socket form", url: "mysql+unix://root@/nonexistent/mysqld.sock"},
		{name: "driver socket form", url: "mariadb://root@unix(/nonexistent/mysqld.sock)/"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			conn, err := dbschema.ConnectToDatabase(t.Context(), test.url)

			c.Assert(err, qt.ErrorMatches, mysqlURLNamesNoDatabase)
			c.Assert(conn, qt.IsNil)
		})
	}
}

// TestConnectToDatabase_ReachesTheDriverForAMySQLURLThatNamesADatabase is the
// control: the same URLs naming a database are dialed, and fail at the dial.
func TestConnectToDatabase_ReachesTheDriverForAMySQLURLThatNamesADatabase(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{name: "URL form", url: "mysql://root@127.0.0.1:1/app"},
		{name: "socket form", url: "mysql+unix://root@/nonexistent/mysqld.sock?database=app"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			conn, err := dbschema.ConnectToDatabase(t.Context(), test.url)

			c.Assert(err, qt.ErrorMatches, `(?s)failed to ping database: .*`)
			c.Assert(conn, qt.IsNil)
		})
	}
}

// TestConnectToDatabase_ReachesTheDriverForAPostgreSQLURLThatNamesNoDatabase
// keeps the refusal to the MySQL family. A PostgreSQL session with no database
// in its URL selects the database named for the user, so the URL is dialed and
// fails at the dial.
func TestConnectToDatabase_ReachesTheDriverForAPostgreSQLURLThatNamesNoDatabase(t *testing.T) {
	c := qt.New(t)

	conn, err := dbschema.ConnectToDatabase(t.Context(), "postgres://root@127.0.0.1:1")

	c.Assert(err, qt.ErrorMatches, `(?s)failed to ping database: .*`)
	c.Assert(conn, qt.IsNil)
}

//go:build integration

package dbschema_test

import (
	"net/url"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// storedValuesEngines are the MySQL-family servers a loose session is opened
// on.
var storedValuesEngines = []struct {
	name   string
	engine dbtarget.Engine
}{
	{name: "mysql", engine: dbtarget.MySQL},
	{name: "mariadb", engine: dbtarget.MariaDB},
}

// looseSessionMode names no strict mode. It names ANSI_QUOTES, which decides
// how the server reads a statement and stays under the strict mode, and
// NO_ZERO_DATE, whose effect a strict mode changes and which is taken out.
const looseSessionMode = "'ANSI_QUOTES,NO_ZERO_DATE,NO_ENGINE_SUBSTITUTION'"

// connectLoose connects to engine's database with every session under
// [looseSessionMode]. The address may name its host as tcp(host:port), which
// url.Parse refuses, so only the query is parsed.
func connectLoose(c *qt.C, engine dbtarget.Engine) *dbschema.DatabaseConnection {
	c.Helper()
	address, rawQuery, _ := strings.Cut(dbtarget.URL(c, engine), "?")
	query, err := url.ParseQuery(rawQuery)
	c.Assert(err, qt.IsNil)
	query.Set("sql_mode", looseSessionMode)
	conn, err := dbschema.ConnectToDatabase(c.Context(), address+"?"+query.Encode())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// sessionMode reads the sql_mode of conn's session.
func sessionMode(c *qt.C, conn *dbschema.DatabaseConnection) string {
	c.Helper()
	var mode string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT @@SESSION.sql_mode").Scan(&mode), qt.IsNil)
	return mode
}

// A pinned session runs the callback under a strict mode that keeps
// ANSI_QUOTES, and has its own mode again afterward, so what the session runs
// next reads its statements as it did before.
func TestWithStoredValuesKept_LiveMySQLFamilyPinnedSessionGetsItsModeBack(t *testing.T) {
	for _, row := range storedValuesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectLoose(c, row.engine)
			var before, during, after string

			err := conn.WithSession(c.Context(), func(pinned *dbschema.DatabaseConnection) error {
				before = sessionMode(c, pinned)
				c.Assert(pinned.WithStoredValuesKept(c.Context(), func(kept *dbschema.DatabaseConnection) error {
					during = sessionMode(c, kept)
					return nil
				}), qt.IsNil)
				after = sessionMode(c, pinned)
				return nil
			})

			c.Assert(err, qt.IsNil)
			c.Assert(before, qt.Equals, "ANSI_QUOTES,NO_ZERO_DATE,NO_ENGINE_SUBSTITUTION")
			c.Assert(during, qt.Equals, "ANSI_QUOTES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION")
			c.Assert(after, qt.Equals, before)
		})
	}
}

// sessionGone reports whether the server's process list holds no connection
// with id, asking for up to ten seconds: a connection the client closed
// leaves the list shortly after, not at once.
func sessionGone(c *qt.C, conn *dbschema.DatabaseConnection, id int64) bool {
	c.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var count int
		c.Assert(conn.QueryRowContext(c.Context(),
			"SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID = ?", id).Scan(&count), qt.IsNil)
		if count == 0 || time.Now().After(deadline) {
			return count == 0
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A connection that is not pinned runs the callback on a session it pins for
// it, and closes that session afterward rather than give it back to the pool,
// so no later statement reaches a session whose mode it changed.
func TestWithStoredValuesKept_LiveMySQLFamilyClosesTheSessionItPinned(t *testing.T) {
	for _, row := range storedValuesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectLoose(c, row.engine)
			var id int64
			var during string

			err := conn.WithStoredValuesKept(c.Context(), func(kept *dbschema.DatabaseConnection) error {
				during = sessionMode(c, kept)
				return kept.QueryRowContext(c.Context(), "SELECT CONNECTION_ID()").Scan(&id)
			})

			c.Assert(err, qt.IsNil)
			c.Assert(during, qt.Equals, "ANSI_QUOTES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION")
			c.Assert(sessionGone(c, conn, id), qt.IsTrue)
			c.Assert(sessionMode(c, conn), qt.Equals, "ANSI_QUOTES,NO_ZERO_DATE,NO_ENGINE_SUBSTITUTION")
		})
	}
}

// A statement that would rewrite a value a row stores fails on the session,
// and the error names the column, says why, and keeps the server's own error.
// The table is temporary, so it lives and ends with the session.
func TestWithStoredValuesKept_LiveMySQLFamilyRefusesARewrite(t *testing.T) {
	for _, row := range storedValuesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectLoose(c, row.engine)

			err := conn.WithStoredValuesKept(c.Context(), func(kept *dbschema.DatabaseConnection) error {
				_, createErr := kept.ExecContext(c.Context(), "CREATE TEMPORARY TABLE stored_values_probe (id INT PRIMARY KEY, n ENUM('happy','ok'))")
				c.Assert(createErr, qt.IsNil)
				_, insertErr := kept.ExecContext(c.Context(), "INSERT INTO stored_values_probe VALUES (1, 'ok')")
				c.Assert(insertErr, qt.IsNil)
				_, alterErr := kept.ExecContext(c.Context(), "ALTER TABLE stored_values_probe MODIFY COLUMN n ENUM('happy')")
				return alterErr
			})

			c.Assert(err, qt.ErrorIs, dbschema.ErrStoredValuesRewritten)
			c.Assert(err, qt.ErrorMatches, `column "n": the change would rewrite stored values: Error 1265 \(01000\): Data truncated for column 'n' at row 1`)
			var server *mysqldriver.MySQLError
			c.Assert(err, qt.ErrorAs, &server)
			c.Assert(server.Number, qt.Equals, uint16(1265))
		})
	}
}

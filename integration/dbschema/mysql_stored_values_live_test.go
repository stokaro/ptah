//go:build integration

package dbschema_test

import (
	"context"
	"errors"
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

// ownSessionMode and strictSessionMode are how a session under
// [looseSessionMode] reads its own mode, and the mode the strict window gives
// it.
const (
	ownSessionMode    = "ANSI_QUOTES,NO_ZERO_DATE,NO_ENGINE_SUBSTITUTION"
	strictSessionMode = "ANSI_QUOTES,STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION"
)

// connectLoose connects to engine's database with every session under
// [looseSessionMode].
func connectLoose(c *qt.C, engine dbtarget.Engine) *dbschema.DatabaseConnection {
	c.Helper()
	return connectWithMode(c, engine, looseSessionMode)
}

// connectWithMode connects to engine's database with every session under
// mode, an expression the driver sets the session's mode to. The address may
// name its host as tcp(host:port), which url.Parse refuses, so only the query
// is parsed.
func connectWithMode(c *qt.C, engine dbtarget.Engine, mode string) *dbschema.DatabaseConnection {
	c.Helper()
	address, rawQuery, _ := strings.Cut(dbtarget.URL(c, engine), "?")
	query, err := url.ParseQuery(rawQuery)
	c.Assert(err, qt.IsNil)
	query.Set("sql_mode", mode)
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

			c.Assert(err, qt.ErrorIs, dbschema.ErrValueChangedToFit)
			c.Assert(err, qt.ErrorMatches, `column "n": the server would change a value to fit its column: Error 1265 \(01000\): Data truncated for column 'n' at row 1`)
			var server *mysqldriver.MySQLError
			c.Assert(err, qt.ErrorAs, &server)
			c.Assert(server.Number, qt.Equals, uint16(1265))
		})
	}
}

// A session that is strict already is left as it is, and a statement the
// server refuses there comes back with the server's own error: the window
// changed nothing, so nothing says it did.
func TestWithStoredValuesKept_LiveMySQLFamilyStrictSessionKeepsTheServersError(t *testing.T) {
	for _, row := range storedValuesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectWithMode(c, row.engine, "'STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION'")
			var during string

			err := conn.WithStoredValuesKept(c.Context(), func(kept *dbschema.DatabaseConnection) error {
				during = sessionMode(c, kept)
				_, createErr := kept.ExecContext(c.Context(), "CREATE TEMPORARY TABLE stored_values_probe (id INT PRIMARY KEY, n ENUM('happy','ok'))")
				c.Assert(createErr, qt.IsNil)
				_, insertErr := kept.ExecContext(c.Context(), "INSERT INTO stored_values_probe VALUES (1, 'ok')")
				c.Assert(insertErr, qt.IsNil)
				_, alterErr := kept.ExecContext(c.Context(), "ALTER TABLE stored_values_probe MODIFY COLUMN n ENUM('happy')")
				return alterErr
			})

			c.Assert(during, qt.Equals, "STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION")
			c.Assert(err, qt.Not(qt.ErrorIs), dbschema.ErrValueChangedToFit)
			c.Assert(err, qt.ErrorMatches, `Error 1265 \(01000\): Data truncated for column 'n' at row 1`)
		})
	}
}

// storedModeTable is the table the trigger of [storedBodies] is defined on.
const storedModeTable = "CREATE TABLE stored_values_mode (id INT PRIMARY KEY, code VARCHAR(5))"

// storedBodies are objects the server keeps a session's sql_mode with: the
// statement that defines one, and the query that reads the mode it kept.
var storedBodies = []struct {
	name, create, mode string
}{
	{
		name:   "a trigger",
		create: "CREATE TRIGGER stored_values_mode_bi BEFORE INSERT ON stored_values_mode FOR EACH ROW SET NEW.code = CONCAT(NEW.code, '-x')",
		mode:   "SELECT SQL_MODE FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME = 'stored_values_mode_bi'",
	},
	{
		name:   "a procedure",
		create: "CREATE PROCEDURE stored_values_mode_p() SELECT 1",
		mode:   "SELECT SQL_MODE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() AND ROUTINE_NAME = 'stored_values_mode_p'",
	},
	{
		name:   "an event",
		create: "CREATE EVENT stored_values_mode_e ON SCHEDULE EVERY 1 DAY DISABLE DO SELECT 1",
		mode:   "SELECT SQL_MODE FROM information_schema.EVENTS WHERE EVENT_SCHEMA = DATABASE() AND EVENT_NAME = 'stored_values_mode_e'",
	},
}

// withStoredModeObjects creates the table of [storedBodies] on conn and drops
// it and every body when the test ends, so a run that failed halfway leaves
// nothing for the next.
func withStoredModeObjects(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	drops := []string{
		"DROP TABLE IF EXISTS stored_values_mode",
		"DROP PROCEDURE IF EXISTS stored_values_mode_p",
		"DROP EVENT IF EXISTS stored_values_mode_e",
	}
	for _, drop := range drops {
		_, err := conn.ExecContext(c.Context(), drop)
		c.Assert(err, qt.IsNil)
	}
	c.Cleanup(func() {
		for _, drop := range drops {
			_, err := conn.ExecContext(context.WithoutCancel(c.Context()), drop)
			c.Check(err, qt.IsNil)
		}
	})
	_, err := conn.ExecContext(c.Context(), storedModeTable)
	c.Assert(err, qt.IsNil)
}

// storedMode answers query, which reads the sql_mode an object kept.
func storedMode(c *qt.C, conn *dbschema.DatabaseConnection, query string) string {
	c.Helper()
	var mode string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&mode), qt.IsNil)
	return mode
}

// An object defined on the strict session keeps the mode the session had
// before the window, as it would without the window, and the session is
// strict again for the statement after it.
func TestWithStoredValuesKept_LiveMySQLFamilyBodyKeepsTheSessionsMode(t *testing.T) {
	for _, row := range storedValuesEngines {
		for _, body := range storedBodies {
			t.Run(row.name+"/"+body.name, func(t *testing.T) {
				c := qt.New(t)
				conn := connectLoose(c, row.engine)
				withStoredModeObjects(c, conn)
				var after string

				err := conn.WithStoredValuesKept(c.Context(), func(kept *dbschema.DatabaseConnection) error {
					sendErr := kept.Writer().ExecuteSQL(c.Context(), body.create)
					after = sessionMode(c, kept)
					return sendErr
				})

				c.Assert(err, qt.IsNil)
				c.Assert(after, qt.Equals, strictSessionMode)
				c.Assert(storedMode(c, conn, body.mode), qt.Equals, ownSessionMode)
			})
		}
	}
}

// sendThroughWriter, sendInTransaction and sendThroughExecContext are the
// paths a statement takes to the session inside the window.
func sendThroughWriter(ctx context.Context, kept *dbschema.DatabaseConnection, statement string) error {
	return kept.Writer().ExecuteSQL(ctx, statement)
}

func sendInTransaction(ctx context.Context, kept *dbschema.DatabaseConnection, statement string) error {
	tx, err := kept.SchemaWriter().BeginTransaction(ctx)
	if err != nil {
		return err
	}
	if err := tx.ExecuteSQL(ctx, statement); err != nil {
		return errors.Join(err, tx.Rollback())
	}
	return tx.Commit()
}

func sendThroughExecContext(ctx context.Context, kept *dbschema.DatabaseConnection, statement string) error {
	_, err := kept.ExecContext(ctx, statement)
	return err
}

// A trigger keeps the session's own mode whichever path its statement takes,
// so an insert the application's loose session runs after the plan stores
// the value the trigger cuts to fit, rather than failing under a strict mode
// the trigger kept.
func TestWithStoredValuesKept_LiveMySQLFamilyTriggerKeepsTheSessionsModeOnEveryPath(t *testing.T) {
	paths := []struct {
		name string
		send func(context.Context, *dbschema.DatabaseConnection, string) error
	}{
		{name: "the writer", send: sendThroughWriter},
		{name: "a transaction", send: sendInTransaction},
		{name: "ExecContext", send: sendThroughExecContext},
	}
	for _, row := range storedValuesEngines {
		for _, path := range paths {
			t.Run(row.name+"/"+path.name, func(t *testing.T) {
				c := qt.New(t)
				conn := connectLoose(c, row.engine)
				withStoredModeObjects(c, conn)

				err := conn.WithStoredValuesKept(c.Context(), func(kept *dbschema.DatabaseConnection) error {
					return path.send(c.Context(), kept, storedBodies[0].create)
				})

				c.Assert(err, qt.IsNil)
				c.Assert(storedMode(c, conn, storedBodies[0].mode), qt.Equals, ownSessionMode)
				_, insertErr := conn.ExecContext(c.Context(), "INSERT INTO stored_values_mode VALUES (1, 'abcd')")
				c.Assert(insertErr, qt.IsNil)
				c.Assert(storedMode(c, conn, "SELECT code FROM stored_values_mode WHERE id = 1"), qt.Equals, "abcd-")
			})
		}
	}
}

// createInExecutorWindow pins a session of conn, begins a transaction on it,
// and runs statement in the strict window of the connection whose executor is
// that transaction, the shape a migration callback has.
func createInExecutorWindow(ctx context.Context, conn *dbschema.DatabaseConnection, statement string) error {
	return conn.WithSession(ctx, func(pinned *dbschema.DatabaseConnection) error {
		tx, err := pinned.SchemaWriter().BeginTransaction(ctx)
		if err != nil {
			return err
		}
		err = pinned.WithExecutor(tx).WithStoredValuesKept(ctx, func(kept *dbschema.DatabaseConnection) error {
			return kept.Writer().ExecuteSQL(ctx, statement)
		})
		if err != nil {
			return errors.Join(err, tx.Rollback())
		}
		return tx.Commit()
	})
}

// A connection whose executor is a transaction runs the trigger's statement
// through that executor, and the trigger keeps the session's own mode there
// too.
func TestWithStoredValuesKept_LiveMySQLFamilyTriggerKeepsTheSessionsModeThroughAnExecutor(t *testing.T) {
	for _, row := range storedValuesEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectLoose(c, row.engine)
			withStoredModeObjects(c, conn)

			err := createInExecutorWindow(c.Context(), conn, storedBodies[0].create)

			c.Assert(err, qt.IsNil)
			c.Assert(storedMode(c, conn, storedBodies[0].mode), qt.Equals, ownSessionMode)
		})
	}
}

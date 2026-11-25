//go:build integration

package dbschema_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	mysqldriver "github.com/go-sql-driver/mysql"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/mysqlstored"
)

// storedSpellingEngines are the MySQL-family servers, each with the
// administrative account that may create an account and name it as a
// definer.
var storedSpellingEngines = []struct {
	name          string
	engine, admin dbtarget.Engine
}{
	{name: "mysql", engine: dbtarget.MySQL, admin: dbtarget.MySQLAdmin},
	{name: "mariadb", engine: dbtarget.MariaDB, admin: dbtarget.MariaDBAdmin},
}

// connectAdminLoose connects to engine's database with its administrative
// account, every session under [looseSessionMode]. The administrative address
// names another database, so the database is taken from engine's address.
func connectAdminLoose(c *qt.C, engine, admin dbtarget.Engine) *dbschema.DatabaseConnection {
	c.Helper()
	address, query, _ := strings.Cut(dbtarget.URL(c, admin), "?")
	database, _, _ := strings.Cut(dbtarget.URL(c, engine), "?")
	database = database[strings.LastIndex(database, "/")+1:]
	return connectURLWithMode(c, address[:strings.LastIndex(address, "/")+1]+database+"?"+query, looseSessionMode)
}

// dottedDefiner is an account whose host has several dotted parts, which
// MySQL and MariaDB accept unquoted in a DEFINER clause.
const dottedDefiner = "'stored_values_trg'@'localhost.localdomain'"

// withDottedDefiner creates [dottedDefiner] on conn's server, locked so that
// nothing signs in as it, with the privileges a trigger it defines needs on
// conn's database, and drops it when the test ends.
func withDottedDefiner(c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	var database string
	c.Assert(conn.QueryRowContext(c.Context(), "SELECT DATABASE()").Scan(&database), qt.IsNil)
	drop := "DROP USER IF EXISTS " + dottedDefiner
	for _, statement := range []string{
		drop,
		"CREATE USER " + dottedDefiner + " ACCOUNT LOCK",
		"GRANT ALL ON `" + strings.ReplaceAll(database, "`", "``") + "`.* TO " + dottedDefiner,
	} {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.WithoutCancel(c.Context()), drop)
		c.Check(err, qt.IsNil)
	})
}

// A trigger whose DEFINER clause spells the account the way MySQL and
// MariaDB accept it keeps the session's own mode, and the application's
// loose insert stores the value the trigger cuts to fit. Without the window
// recognizing the spelling, the trigger keeps STRICT_ALL_TABLES and the
// insert fails with error 1406.
func TestWithStoredValuesKept_LiveMySQLFamilySpelledTriggerKeepsTheSessionsMode(t *testing.T) {
	triggers := []struct {
		name, create string
	}{
		{
			name: "a definer host of dotted parts",
			create: "CREATE DEFINER=stored_values_trg@localhost.localdomain TRIGGER stored_values_mode_bi " +
				"BEFORE INSERT ON stored_values_mode FOR EACH ROW SET NEW.code = CONCAT(NEW.code, '-x')",
		},
		{
			name: "executable comments as mysqldump writes them",
			create: "/*!50003 CREATE*/ /*!50017 DEFINER=`stored_values_trg`@`localhost.localdomain`*/ /*!50003 TRIGGER " +
				"stored_values_mode_bi BEFORE INSERT ON stored_values_mode FOR EACH ROW SET NEW.code = CONCAT(NEW.code, '-x') */",
		},
	}
	for _, row := range storedSpellingEngines {
		for _, trigger := range triggers {
			t.Run(row.name+"/"+trigger.name, func(t *testing.T) {
				c := qt.New(t)
				admin := connectAdminLoose(c, row.engine, row.admin)
				withStoredModeObjects(c, admin)
				withDottedDefiner(c, admin)

				err := admin.WithStoredValuesKept(c.Context(), func(kept *dbschema.DatabaseConnection) error {
					return kept.Writer().ExecuteSQL(c.Context(), trigger.create)
				})

				c.Assert(err, qt.IsNil)
				c.Assert(storedMode(c, admin, storedBodies[0].mode), qt.Equals, ownSessionMode)
				_, insertErr := admin.ExecContext(c.Context(), "INSERT INTO stored_values_mode VALUES (1, 'abcd')")
				c.Assert(insertErr, qt.IsNil)
				c.Assert(storedMode(c, admin, "SELECT code FROM stored_values_mode WHERE id = 1"), qt.Equals, "abcd-")
			})
		}
	}
}

// An ALTER EVENT that names a definer and writes DO stores the session's
// mode with the event, measured on MySQL 8.4 and MariaDB 11.8, and so runs
// under the session's own mode. The event is created on a strict session
// first, so the mode it ends with is the ALTER's.
func TestWithStoredValuesKept_LiveMySQLFamilyEventADefinerAltersKeepsTheSessionsMode(t *testing.T) {
	for _, row := range storedSpellingEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectLoose(c, row.engine)
			withStoredModeObjects(c, conn)
			strict := connectWithMode(c, row.engine, "'STRICT_ALL_TABLES'")
			_, err := strict.ExecContext(c.Context(), storedBodies[2].create)
			c.Assert(err, qt.IsNil)
			c.Assert(storedMode(c, conn, storedBodies[2].mode), qt.Equals, "STRICT_ALL_TABLES")

			err = conn.WithStoredValuesKept(c.Context(), func(kept *dbschema.DatabaseConnection) error {
				return kept.Writer().ExecuteSQL(c.Context(), "ALTER DEFINER=CURRENT_USER EVENT stored_values_mode_e DO SELECT 2")
			})

			c.Assert(err, qt.IsNil)
			c.Assert(storedMode(c, conn, storedBodies[2].mode), qt.Equals, ownSessionMode)
		})
	}
}

// MariaDB keeps the session's mode with a package and a package body, and
// runs a `/*M! ... */` comment, which MySQL reads as an ordinary one. Each
// object keeps the session's own mode.
func TestWithStoredValuesKept_LiveMariaDBBodyKeepsTheSessionsMode(t *testing.T) {
	bodies := []struct {
		name       string
		statements []string
		modes      []string
	}{
		{
			name: "a package and its body",
			statements: []string{
				"CREATE OR REPLACE PACKAGE stored_values_mode_pkg PROCEDURE p(); END",
				"CREATE DEFINER=CURRENT_USER PACKAGE BODY stored_values_mode_pkg PROCEDURE p() BEGIN SELECT 1; END; END",
			},
			modes: []string{
				"SELECT SQL_MODE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() " +
					"AND ROUTINE_NAME = 'stored_values_mode_pkg' AND ROUTINE_TYPE = 'PACKAGE'",
				"SELECT SQL_MODE FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = DATABASE() " +
					"AND ROUTINE_NAME = 'stored_values_mode_pkg' AND ROUTINE_TYPE = 'PACKAGE BODY'",
			},
		},
		{
			name:       "a procedure in a MariaDB comment",
			statements: []string{"/*M!100100 CREATE PROCEDURE stored_values_mode_p() SELECT 1 */"},
			modes:      []string{storedBodies[1].mode},
		},
	}
	for _, body := range bodies {
		t.Run(body.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectLoose(c, dbtarget.MariaDB)
			withStoredModeObjects(c, conn)
			withDroppedAtEnd(c, conn, "DROP PACKAGE IF EXISTS stored_values_mode_pkg")

			err := conn.WithStoredValuesKept(c.Context(), func(kept *dbschema.DatabaseConnection) error {
				return sendAll(c.Context(), kept, body.statements)
			})

			c.Assert(err, qt.IsNil)
			for _, mode := range body.modes {
				c.Assert(storedMode(c, conn, mode), qt.Equals, ownSessionMode)
			}
		})
	}
}

// sendAll sends statements through the window's writer in order, and stops
// at the first that fails.
func sendAll(ctx context.Context, kept *dbschema.DatabaseConnection, statements []string) error {
	for _, statement := range statements {
		if err := kept.Writer().ExecuteSQL(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

// withDroppedAtEnd runs drop on conn at once and again when the test ends.
func withDroppedAtEnd(c *qt.C, conn *dbschema.DatabaseConnection, drop string) {
	c.Helper()
	_, err := conn.ExecContext(c.Context(), drop)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := conn.ExecContext(context.WithoutCancel(c.Context()), drop)
		c.Check(err, qt.IsNil)
	})
}

// A statement whose executable comment the window cannot read is refused
// before any of it is sent. The server accepts this one: it reads the string
// '*/' as a string and ends the comment at the */ after it, where the lexer
// ends it inside the string, so the window cannot tell what it defines.
func TestWithStoredValuesKept_LiveMySQLFamilyRefusesABodyItCannotRead(t *testing.T) {
	for _, row := range storedSpellingEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectLoose(c, row.engine)
			withStoredModeObjects(c, conn)

			err := conn.WithStoredValuesKept(c.Context(), func(kept *dbschema.DatabaseConnection) error {
				return kept.Writer().ExecuteSQL(c.Context(), "/*!50003 CREATE TRIGGER stored_values_mode_bi "+
					"BEFORE INSERT ON stored_values_mode FOR EACH ROW SET NEW.code = '*/' */")
			})

			c.Assert(err, qt.ErrorIs, mysqlstored.ErrUnreadableComment)
			c.Assert(storedMode(c, conn, "SELECT CAST(COUNT(*) AS CHAR) FROM information_schema.TRIGGERS "+
				"WHERE TRIGGER_SCHEMA = DATABASE() AND TRIGGER_NAME = 'stored_values_mode_bi'"), qt.Equals, "0")
		})
	}
}

// A body statement that fails still sets the strict mode again after it, so
// the ALTER after it, which would cut a value a row stores, is refused and
// the row keeps its value.
func TestWithStoredValuesKept_LiveMySQLFamilyStrictAgainAfterAFailedBody(t *testing.T) {
	for _, row := range storedSpellingEngines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectLoose(c, row.engine)
			withStoredModeObjects(c, conn)
			_, err := conn.ExecContext(c.Context(), "INSERT INTO stored_values_mode VALUES (1, 'abcd')")
			c.Assert(err, qt.IsNil)
			var bodyErr error

			err = conn.WithStoredValuesKept(c.Context(), func(kept *dbschema.DatabaseConnection) error {
				bodyErr = kept.Writer().ExecuteSQL(c.Context(), "CREATE TRIGGER stored_values_mode_bi "+
					"BEFORE INSERT ON stored_values_missing FOR EACH ROW SET NEW.code = 'x'")
				return kept.Writer().ExecuteSQL(c.Context(), "ALTER TABLE stored_values_mode MODIFY code VARCHAR(2)")
			})

			var server *mysqldriver.MySQLError
			c.Assert(bodyErr, qt.ErrorAs, &server)
			c.Assert(server.Number, qt.Equals, uint16(1146))
			c.Assert(err, qt.ErrorIs, dbschema.ErrValueChangedToFit)
			c.Assert(storedMode(c, conn, "SELECT code FROM stored_values_mode WHERE id = 1"), qt.Equals, "abcd")
		})
	}
}

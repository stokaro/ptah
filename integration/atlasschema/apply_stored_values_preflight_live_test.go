//go:build integration

package atlasschema_test

import (
	"context"
	"net/url"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/mysqlstored"
	"ptah.run/migration/migrator"
)

// The batch `schema apply` sends runs on a session that refuses a statement it
// cannot read. The batch is read whole before the first statement is sent, so
// such a statement refuses the batch with nothing of it applied. MySQL and
// MariaDB run DDL outside a transaction: without the read, the table the
// first statement creates is there after the refusal. The session starts
// with no strict mode, since a strict session needs no window and reads
// nothing.
func TestApplyStatements_LiveMySQLFamilyRefusesAnUnreadableStatementBeforeSending(t *testing.T) {
	engines := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "mysql", engine: dbtarget.MySQL},
		{name: "mariadb", engine: dbtarget.MariaDB},
	}
	batch := []string{
		"CREATE TABLE apply_preflight_first (id INT)",
		"CREATE TABLE apply_preflight_t (code VARCHAR(5))",
		"/*!50003 CREATE TRIGGER apply_preflight_bi BEFORE INSERT ON apply_preflight_t FOR EACH ROW SET NEW.code = '*/' */",
	}
	for _, row := range engines {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn := connectWithoutStrictMode(c, dbtarget.URL(c, row.engine))
			dropPreflightTables(c.Context(), c, conn)
			c.Cleanup(func() { dropPreflightTables(context.WithoutCancel(c.Context()), c, conn) })

			err := atlasschema.ApplyStatements(c.Context(), conn, migrator.MigrationTxModeNone, batch)

			c.Assert(err, qt.ErrorIs, mysqlstored.ErrUnreadableComment)
			var tables int
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM information_schema.TABLES "+
				"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 'apply\\_preflight\\_%'").Scan(&tables), qt.IsNil)
			c.Assert(tables, qt.Equals, 0)
		})
	}
}

// connectWithoutStrictMode connects to the database rawURL names with every
// session under a sql_mode that names no strict mode. The address may name its
// host as tcp(host:port), which url.Parse refuses, so only the query is
// parsed.
func connectWithoutStrictMode(c *qt.C, rawURL string) *dbschema.DatabaseConnection {
	c.Helper()
	address, rawQuery, _ := strings.Cut(rawURL, "?")
	query, err := url.ParseQuery(rawQuery)
	c.Assert(err, qt.IsNil)
	query.Set("sql_mode", "'NO_ENGINE_SUBSTITUTION'")
	conn, err := dbschema.ConnectToDatabase(c.Context(), address+"?"+query.Encode())
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

// dropPreflightTables drops the tables the batch creates.
func dropPreflightTables(ctx context.Context, c *qt.C, conn *dbschema.DatabaseConnection) {
	c.Helper()
	_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS apply_preflight_first, apply_preflight_t")
	c.Check(err, qt.IsNil)
}

//go:build integration

package dbtest_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	// The scratch database is created and read back through the driver, with
	// nothing of Ptah's in between.
	_ "github.com/go-sql-driver/mysql"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/mysqlstored"
	"ptah.run/migration/dbtest"
)

// preflightEntity declares a table and a trigger on it whose body holds an
// executable comment the strict session cannot read: the string in it runs
// past the */ the comment ends at, and the server reads the string across
// that */. The plan creates the table first.
const preflightEntity = `package models

//ptah:schema:table name="preflight_t"
//ptah:schema:trigger name="preflight_bi" table="preflight_t" timing="before" event="insert" for="row" body="SET NEW.code = /*!50003 '*/' */ 'x'"
type PreflightT struct {
	//ptah:schema:field name="code" type="VARCHAR(5)"
	Code string
}
`

// preflightDatabase creates an empty database on engine's server, dropped
// when the test ends, and returns its name, the URL ptah reaches it with,
// every session under a sql_mode that names no strict mode, and the driver's
// connection to the server.
func preflightDatabase(c *qt.C, engine dbtarget.Engine) (name, dbURL string, admin *sql.DB) {
	c.Helper()
	admin, err := sql.Open("mysql", dbtarget.DriverDSN(c, engine))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name = fmt.Sprintf("ptah_rt_preflight_%d", time.Now().UnixNano())
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, err := admin.ExecContext(context.WithoutCancel(c.Context()), "DROP DATABASE IF EXISTS "+name)
		c.Check(err, qt.IsNil)
	})
	address, rawQuery, _ := strings.Cut(dbtarget.URL(c, engine), "?")
	query, err := url.ParseQuery(rawQuery)
	c.Assert(err, qt.IsNil)
	query.Set("sql_mode", "'NO_ENGINE_SUBSTITUTION'")
	return name, address[:strings.LastIndex(address, "/")+1] + name + "?" + query.Encode(), admin
}

// A desired schema a schema test applies runs on a session that refuses a
// statement it cannot read, and the whole plan is read before the first
// statement is sent. So the trigger refuses the plan with nothing of it
// applied. MySQL and MariaDB run DDL outside a transaction: without the read,
// the table the plan creates first is there after the refusal.
func TestRunSchemaTest_LiveMySQLFamilyRefusesAnUnreadablePlanBeforeSending(t *testing.T) {
	engines := []struct {
		name  string
		admin dbtarget.Engine
	}{
		{name: "mysql", admin: dbtarget.MySQLAdmin},
		{name: "mariadb", admin: dbtarget.MariaDBAdmin},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			rootDir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(rootDir, "preflight.go"), []byte(preflightEntity), 0o600), qt.IsNil)
			database, dbURL, admin := preflightDatabase(c, engine.admin)

			report, err := dbtest.RunSchemaTest(c.Context(), dbtest.SchemaOptions{
				Runtime: builtintest.Runtime(),
				DBURL:   dbURL,
				RootDir: rootDir,
				Cases:   []dbtest.Case{{Name: "provisioned", Steps: []dbtest.Step{{Name: "select", Exec: "SELECT 1"}}}},
			})

			c.Assert(err, qt.ErrorIs, mysqlstored.ErrUnreadableComment)
			c.Assert(report, qt.IsNil)
			var tables int
			c.Assert(admin.QueryRowContext(c.Context(),
				"SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", database).Scan(&tables), qt.IsNil)
			c.Assert(tables, qt.Equals, 0)
		})
	}
}

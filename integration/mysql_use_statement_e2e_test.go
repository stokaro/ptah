//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// useSchemaFile writes a schema file for a whole server that selects each
// database with USE and writes its table names without one: app with t and u,
// u keeping a foreign key into t, and second with v.
func useSchemaFile(c *qt.C, app, second string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte("CREATE DATABASE `"+app+"`;\nUSE `"+app+"`;\n"+
		"CREATE TABLE t (id int PRIMARY KEY, name varchar(20));\n"+
		"CREATE TABLE u (id int PRIMARY KEY, t_id int, CONSTRAINT fk FOREIGN KEY (t_id) REFERENCES t (id));\n"+
		"CREATE DATABASE `"+second+"`;\nUSE `"+second+"`;\n"+
		"CREATE TABLE v (id int PRIMARY KEY);\n"), 0o600), qt.IsNil)
	return path
}

// TestSchemaApplyReadsUseOnAMySQLServerE2E applies a schema file that selects
// its databases with USE to a whole server, and reads the catalog back: each
// table is in the database the USE before it selected, and the foreign key
// written as REFERENCES t points into that database. Measured on MySQL 8.4.11
// with the pinned community binary v1.3.0, the same file compared with the
// server plans the tables that way. Without reading USE, every table names no
// database and the whole-server comparison refuses the file
// (stokaro/ptah#3926). Planned again, the file is synced.
func TestSchemaApplyReadsUseOnAMySQLServerE2E(t *testing.T) {
	for _, engine := range mysqlServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLServerAccount(c, engine.admin, []string{"app", "second"}, nil)
			app, second := server.names["app"], server.names["second"]
			schema := useSchemaFile(c, app, second)

			applied, err := runPtahNativeWithError("schema", "apply", "--db-url", server.url, "--schema-file", schema, "--auto-approve")
			again, againErr := runPtahNativeWithError("schema", "apply", "--db-url", server.url, "--schema-file", schema, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", applied))
			var tables, reference string
			c.Assert(server.admin.QueryRowContext(c.Context(), `
				SELECT
					(SELECT GROUP_CONCAT(CONCAT(TABLE_SCHEMA, '.', TABLE_NAME) ORDER BY TABLE_SCHEMA, TABLE_NAME)
					 FROM information_schema.TABLES WHERE TABLE_SCHEMA IN (?, ?)),
					(SELECT CONCAT(REFERENCED_TABLE_SCHEMA, '.', REFERENCED_TABLE_NAME)
					 FROM information_schema.KEY_COLUMN_USAGE
					 WHERE TABLE_SCHEMA = ? AND TABLE_NAME = 'u' AND REFERENCED_TABLE_NAME IS NOT NULL)`,
				app, second, app).Scan(&tables, &reference), qt.IsNil)
			c.Assert(tables, qt.Equals, app+".t,"+app+".u,"+second+".v")
			c.Assert(reference, qt.Equals, app+".t")
			c.Assert(againErr, qt.IsNil, qt.Commentf("%s", again))
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

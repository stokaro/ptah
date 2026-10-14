//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// serialSchema declares one table whose key column is a SERIAL, and a second
// column a row can be inserted through.
const serialSchema = "tables:\n  things:\n    fields:\n      id:\n        type: SERIAL\n        primary: true\n" +
	"      n:\n        type: INT\n        nullable: true\n"

// serialDatabases creates an empty scratch database on each engine that writes
// a serial type as its own auto-increment integer, and returns its URL.
var serialDatabases = map[string]func(c *qt.C) string{
	"mysql": func(c *qt.C) string {
		return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.MySQL), "DROP DATABASE IF EXISTS %s", renameInPath)
	},
	"mariadb": func(c *qt.C) string {
		return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.MariaDB), "DROP DATABASE IF EXISTS %s", renameInPath)
	},
	"sqlserver": func(c *qt.C) string {
		return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
			"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
	},
}

// TestSerialAutoIncrementE2E_HappyPath applies a SERIAL key, inserts a row
// that names no key, and plans the same schema again.
//
// Measured on master against MySQL 26.7, MariaDB 12.3 and SQL Server 2025, a
// SERIAL key was written as a plain INT, so the insert failed for want of a
// value: the declaration's auto-increment never reached the database. Each
// engine now writes its own form, and the column the server reports as
// auto-increment compares equal to the SERIAL that declared it.
func TestSerialAutoIncrementE2E_HappyPath(t *testing.T) {
	tests := []struct {
		engine   string
		wantPlan string
	}{
		{engine: "mysql", wantPlan: "  `id` INT PRIMARY KEY AUTO_INCREMENT,\n"},
		{engine: "mariadb", wantPlan: "  `id` INT PRIMARY KEY AUTO_INCREMENT,\n"},
		{engine: "sqlserver", wantPlan: "  [id] INT IDENTITY(1,1) PRIMARY KEY,\n"},
	}
	for _, test := range tests {
		t.Run(test.engine, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			dbURL := serialDatabases[test.engine](c)
			schema := writeAutoIncrementSchema(c, "schema.yaml", serialSchema)

			applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", schema, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))
			c.Assert(applied, qt.Contains, test.wantPlan)

			conn := connectDevDialect(c, dbURL)
			_, err = conn.ExecContext(ctx, "INSERT INTO things (n) VALUES (7)")
			c.Assert(err, qt.IsNil)
			var id int64
			c.Assert(conn.QueryRowContext(ctx, "SELECT id FROM things WHERE n = 7").Scan(&id), qt.IsNil)
			c.Assert(id, qt.Equals, int64(1))

			reapplied, reappliedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", schema, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", reapplied, reappliedErr))
			c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestSerialAutoIncrementE2E_FailurePath declares a SERIAL column that no key
// of its table starts with. MySQL refuses an AUTO_INCREMENT column outside a
// key with ERROR 1075, so the apply is refused by name before the table is
// created.
func TestSerialAutoIncrementE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	dbURL := serialDatabases["mysql"](c)
	schema := writeAutoIncrementSchema(c, "schema.yaml",
		"tables:\n  things:\n    fields:\n      id:\n        type: INT\n        primary: true\n      seq:\n        type: SERIAL\n")

	applied, appliedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", schema, "--db-url", dbURL, "--auto-approve",
	})
	c.Assert(err, qt.ErrorMatches, `.*column things\.seq is a serial type, which mysql writes as AUTO_INCREMENT, and no key of the table starts with it; .*`,
		qt.Commentf("stdout:\n%s\nstderr:\n%s", applied, appliedErr))
	c.Assert(applied, qt.Not(qt.Contains), "CREATE TABLE")
}

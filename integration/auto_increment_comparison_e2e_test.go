//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// autoIncrementDatabases creates an empty scratch database on each engine
// whose comparison reads a column's auto-increment, and returns its URL.
var autoIncrementDatabases = map[string]func(c *qt.C) string{
	"mysql": func(c *qt.C) string {
		return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.MySQL), "DROP DATABASE IF EXISTS %s", renameInPath)
	},
	"mariadb": func(c *qt.C) string {
		return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.MariaDB), "DROP DATABASE IF EXISTS %s", renameInPath)
	},
	"sqlite": func(c *qt.C) string {
		return "sqlite://" + filepath.ToSlash(filepath.Join(c.TempDir(), "auto_increment.db"))
	},
	"sqlserver": func(c *qt.C) string {
		return createdDevDialectDatabase(c, dbtarget.URL(c, dbtarget.SQLServer),
			"ALTER DATABASE %[1]s SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE %[1]s", replaceSQLServerDatabase)
	},
}

// TestAutoIncrementComparisonE2E_HappyPath adds an auto-increment to a key
// column that exists, removes it again, and applies each schema twice.
//
// Measured on master against MySQL 26.7, MariaDB 12.3 and SQLite, adding
// AUTO_INCREMENT or AUTOINCREMENT to the declaration reported "Schema is
// synced" and the column never got it: the comparison did not read a column's
// auto-increment. On MySQL and MariaDB the plan has to restate the key column
// without PRIMARY KEY, which the servers refuse on a table that has one; on
// SQLite it rebuilds the table.
func TestAutoIncrementComparisonE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		engine   string
		from     string
		to       string
		wantPlan string
	}{
		{
			name: "MySQL gains it", engine: "mysql",
			from:     "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT);",
			to:       "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, a INT);",
			wantPlan: "ALTER TABLE `t` MODIFY COLUMN `id` INT NOT NULL AUTO_INCREMENT;\n",
		},
		{
			name: "MySQL loses it", engine: "mysql",
			from:     "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, a INT);",
			to:       "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT);",
			wantPlan: "ALTER TABLE `t` MODIFY COLUMN `id` INT NOT NULL;\n",
		},
		{
			name: "MariaDB gains it", engine: "mariadb",
			from:     "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT);",
			to:       "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, a INT);",
			wantPlan: "ALTER TABLE `t` MODIFY COLUMN `id` INT NOT NULL AUTO_INCREMENT;\n",
		},
		{
			name: "MariaDB loses it", engine: "mariadb",
			from:     "CREATE TABLE t (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, a INT);",
			to:       "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT);",
			wantPlan: "ALTER TABLE `t` MODIFY COLUMN `id` INT NOT NULL;\n",
		},
		{
			name: "SQLite gains it", engine: "sqlite",
			from:     "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT);",
			to:       "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, a INT);",
			wantPlan: "CREATE TABLE \"__ptah_rebuild_t\" (\n  \"id\" INTEGER PRIMARY KEY AUTOINCREMENT,\n",
		},
		{
			name: "SQLite loses it", engine: "sqlite",
			from:     "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, a INT);",
			to:       "CREATE TABLE t (id INTEGER PRIMARY KEY, a INT);",
			wantPlan: "CREATE TABLE \"__ptah_rebuild_t\" (\n  \"id\" INTEGER PRIMARY KEY,\n",
		},
		{
			// A key column whose type changes is the other change the
			// restated PRIMARY KEY refused.
			name: "MySQL's key column widens", engine: "mysql",
			from:     "CREATE TABLE t (id INT NOT NULL PRIMARY KEY, a INT);",
			to:       "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a INT);",
			wantPlan: "ALTER TABLE `t` MODIFY COLUMN `id` BIGINT NOT NULL;\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			dbURL := autoIncrementDatabases[test.engine](c)
			from := writeAutoIncrementSchema(c, "from.sql", test.from)
			to := writeAutoIncrementSchema(c, "to.sql", test.to)

			created, createdErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", from, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", created, createdErr))

			changed, changedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", to, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", changed, changedErr))
			c.Assert(changed, qt.Contains, test.wantPlan)

			reapplied, reappliedErr, err := runPtahSplitStreams(ctx, []string{
				"schema", "apply", "--schema-file", to, "--db-url", dbURL, "--auto-approve",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", reapplied, reappliedErr))
			c.Assert(reapplied, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestAutoIncrementComparisonE2E_FailurePath declares IDENTITY on a SQL Server
// key column that exists. ALTER COLUMN has no IDENTITY there, so the change is
// refused by name before any statement runs, and the column is left as it is.
func TestAutoIncrementComparisonE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	dbURL := autoIncrementDatabases["sqlserver"](c)
	const table = "tables:\n  t:\n    fields:\n      id:\n        type: INT\n        primary: true\n"
	from := writeAutoIncrementSchema(c, "from.yaml", table)
	to := writeAutoIncrementSchema(c, "to.yaml", table+"        auto_increment: true\n")
	created, createdErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", from, "--db-url", dbURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", created, createdErr))

	changed, changedErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", to, "--db-url", dbURL, "--auto-approve",
	})
	c.Assert(err, qt.ErrorMatches, `.*SQL Server cannot add IDENTITY to an existing column or remove it from one: `+
		`t\.id changes identity false -> true; .*`, qt.Commentf("stdout:\n%s\nstderr:\n%s", changed, changedErr))

	kept, keptErr, err := runPtahSplitStreams(ctx, []string{
		"schema", "apply", "--schema-file", from, "--db-url", dbURL, "--auto-approve",
	})
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", kept, keptErr))
	c.Assert(kept, qt.Equals, "Schema is synced, no changes to be made.\n")
}

func writeAutoIncrementSchema(c *qt.C, name, content string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(content+"\n"), 0o600), qt.IsNil)
	return path
}

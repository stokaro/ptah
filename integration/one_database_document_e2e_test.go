//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// oneDatabaseTarget is a URL naming one database on an engine whose schema is a
// database, the database's name, and a read of whether the server holds a
// database of some name.
type oneDatabaseTarget struct {
	url, name string
	holds     func(c *qt.C, name string) bool
}

// oneDatabaseTargets are the engines whose URL limits a run to one database.
var oneDatabaseTargets = []struct {
	name string
	make func(c *qt.C) oneDatabaseTarget
}{
	{name: "MySQL", make: func(c *qt.C) oneDatabaseTarget { return mysqlFamilyOneDatabase(c, dbtarget.MySQLAdmin) }},
	{name: "MariaDB", make: func(c *qt.C) oneDatabaseTarget { return mysqlFamilyOneDatabase(c, dbtarget.MariaDBAdmin) }},
	{name: "ClickHouse", make: clickHouseOneDatabase},
}

func mysqlFamilyOneDatabase(c *qt.C, engine dbtarget.Engine) oneDatabaseTarget {
	c.Helper()
	scratch := newMySQLFamilyScratch(c, engine)
	name, url := scratch.database(c, "one_database")
	return oneDatabaseTarget{url: url, name: name, holds: func(c *qt.C, name string) bool {
		c.Helper()
		return databaseCollation(c, scratch, name) != ""
	}}
}

func clickHouseOneDatabase(c *qt.C) oneDatabaseTarget {
	c.Helper()
	url := clickHouseDevDatabase(c).url
	admin, err := sql.Open("clickhouse", url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
	var name string
	c.Assert(admin.QueryRowContext(c.Context(), `SELECT currentDatabase()`).Scan(&name), qt.IsNil)
	return oneDatabaseTarget{url: url, name: name, holds: func(c *qt.C, name string) bool {
		c.Helper()
		var count uint64
		c.Assert(admin.QueryRowContext(context.Background(), `SELECT count() FROM system.databases WHERE name = ?`, name).Scan(&count), qt.IsNil)
		return count == 1
	}}
}

// oneDatabaseVerbs are the native verbs that plan against a target URL. Each takes
// the URL, the schema file and a directory it may write migrations to.
var oneDatabaseVerbs = []struct {
	name string
	args func(url, file, dir string) []string
}{
	{name: "schema apply", args: func(url, file, _ string) []string {
		return []string{"schema", "apply", "--db-url", url, "--schema-file", file, "--auto-approve"}
	}},
	{name: "schema compare", args: func(url, file, _ string) []string {
		return []string{"schema", "compare", "--db-url", url, "--schema-file", file}
	}},
	{name: "schema drift", args: func(url, file, _ string) []string {
		return []string{"schema", "drift", "--db-url", url, "--schema-file", file}
	}},
	{name: "migrations generate", args: func(url, file, dir string) []string {
		return []string{"migrations", "generate", "--db-url", url, "--schema-file", file, "--migrations-dir", dir}
	}},
	{name: "migrations plan", args: func(url, file, _ string) []string {
		return []string{"migrations", "plan", "--db-url", url, "--schema-file", file}
	}},
}

// writeOneDatabaseDocument writes body under name and returns its path.
func writeOneDatabaseDocument(c *qt.C, name, body string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), name)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// TestDocumentNamingAnotherDatabaseIsRefusedE2E runs each verb on a document that
// declares a database the URL does not name. Each refuses before it plans. A read
// of the one database finds nothing for it, and nothing plans a database, so each
// would otherwise report the document synced. The catalog and the migration
// directory say that nothing happened.
func TestDocumentNamingAnotherDatabaseIsRefusedE2E(t *testing.T) {
	documents := []struct {
		name string
		body func(database string) string
	}{
		{name: "schema.hcl", body: func(database string) string { return fmt.Sprintf("schema %q {\n}\n", database) }},
		{name: "schema.sql", body: func(database string) string { return fmt.Sprintf("CREATE DATABASE %s;\n", database) }},
	}
	for _, target := range oneDatabaseTargets {
		for _, verb := range oneDatabaseVerbs {
			for _, document := range documents {
				t.Run(target.name+"/"+verb.name+"/"+document.name, func(t *testing.T) {
					c := qt.New(t)
					fixture := target.make(c)
					absent := fmt.Sprintf("ptah_one_database_%d", time.Now().UnixNano())
					dir := c.TempDir()
					file := writeOneDatabaseDocument(c, document.name, document.body(absent))

					got := runPtahIn(c, verb.args(fixture.url, file, dir)...)

					c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
					c.Assert(got.Stderr, qt.Contains, fmt.Sprintf("the document declares database %q", absent))
					c.Assert(fixture.holds(c, absent), qt.IsFalse)
					files, err := os.ReadDir(dir)
					c.Assert(err, qt.IsNil)
					c.Assert(files, qt.HasLen, 0)
				})
			}
		}
	}
}

// TestDocumentNamingItsOwnDatabaseIsSyncedE2E is the control: the same verbs on a
// document that declares the database the URL names plan nothing, so the refusal
// is about the name and not about the block.
func TestDocumentNamingItsOwnDatabaseIsSyncedE2E(t *testing.T) {
	for _, target := range oneDatabaseTargets {
		for _, verb := range oneDatabaseVerbs {
			t.Run(target.name+"/"+verb.name, func(t *testing.T) {
				c := qt.New(t)
				fixture := target.make(c)
				dir := c.TempDir()
				file := writeOneDatabaseDocument(c, "schema.hcl", fmt.Sprintf("schema %q {\n}\n", fixture.name))

				got := runPtahIn(c, verb.args(fixture.url, file, dir)...)

				c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
				c.Assert(got.Stderr, qt.Not(qt.Contains), "the document declares database")
				files, err := os.ReadDir(dir)
				c.Assert(err, qt.IsNil)
				c.Assert(files, qt.HasLen, 0)
			})
		}
	}
}

// TestDocumentPuttingATableInAnotherDatabaseIsRefusedE2E is the refusal for a table.
// On ClickHouse a table written in another database that exists is not read back,
// so without it every later run plans the same CREATE TABLE. The database exists
// here, so only the refusal keeps the table out of it.
func TestDocumentPuttingATableInAnotherDatabaseIsRefusedE2E(t *testing.T) {
	c := qt.New(t)
	fixture := clickHouseOneDatabase(c)
	other := clickHouseDevDatabase(c).url
	admin, err := sql.Open("clickhouse", other)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Assert(admin.Close(), qt.IsNil) })
	var name string
	c.Assert(admin.QueryRowContext(c.Context(), `SELECT currentDatabase()`).Scan(&name), qt.IsNil)
	file := writeOneDatabaseDocument(c, "schema.sql", fmt.Sprintf("CREATE TABLE %s.t (id Int32) ENGINE = MergeTree ORDER BY id;\n", name))

	got := runPtahIn(c, "schema", "apply", "--db-url", fixture.url, "--schema-file", file, "--auto-approve")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Contains, fmt.Sprintf("table %q is in database %q", name+".t", name))
	var tables uint64
	c.Assert(admin.QueryRowContext(c.Context(), `SELECT count() FROM system.tables WHERE database = currentDatabase()`).Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, uint64(0))
}

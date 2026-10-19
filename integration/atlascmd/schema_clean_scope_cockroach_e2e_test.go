//go:build integration

package atlas_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestSchemaCleanScopeRetriesASelectedCockroachViewDependency drives a
// selected `ptah-compat schema clean` on CockroachDB, measured on v26.3.2,
// which commits each drop as it runs. The plan drops a_base first, which the
// selected z_child reads. The server refuses it, and the drop is sent again
// once z_child is gone. The table the views read is not selected and stays.
func TestSchemaCleanScopeRetriesASelectedCockroachViewDependency(t *testing.T) {
	c := qt.New(t)
	dbURL := crdbScopedCleanDatabase(c, "view_retry")
	crdbScopedCleanExec(c, dbURL,
		"CREATE TABLE base_t (id INT PRIMARY KEY)",
		"CREATE VIEW a_base AS SELECT id FROM base_t",
		"CREATE VIEW z_child AS SELECT id FROM a_base",
	)

	compat := buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat")
	stdout, stderr, code := runAtlasBinary(
		compat,
		nil,
		"schema", "clean",
		"--url", dbURL,
		"--include", "*[type=view]",
		"--auto-approve",
	)

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, `DROP VIEW IF EXISTS "a_base" RESTRICT`)
	c.Assert(stdout, qt.Contains, `DROP VIEW IF EXISTS "z_child" RESTRICT`)
	c.Assert(stderr, qt.Equals, "")
	c.Assert(crdbScopedCleanRelations(c, dbURL), qt.DeepEquals, []string{"base_t"})
}

// TestSchemaCleanScopeKeepsEarlierCockroachDropsWhenRestrictRefuses selects a
// table nothing reads and an enum that an unselected table uses, on
// CockroachDB v26.3.2. The table is dropped first and stays dropped, because
// the server commits each drop as it runs. The server refuses the enum, and
// the error names the table dropped before the refusal.
func TestSchemaCleanScopeKeepsEarlierCockroachDropsWhenRestrictRefuses(t *testing.T) {
	c := qt.New(t)
	dbURL := crdbScopedCleanDatabase(c, "restrict_refuses")
	crdbScopedCleanExec(c, dbURL,
		"CREATE TABLE free_t (id INT PRIMARY KEY)",
		"CREATE TYPE mood AS ENUM ('calm')",
		"CREATE TABLE keep_t (id INT PRIMARY KEY, m mood)",
	)

	compat := buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat")
	stdout, stderr, code := runAtlasBinary(
		compat,
		nil,
		"schema", "clean",
		"--url", dbURL,
		"--include", "free_t",
		"--include", "mood",
		"--auto-approve",
	)

	c.Assert(code, qt.Equals, 1, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	c.Assert(stdout, qt.Contains, `DROP TABLE IF EXISTS "free_t" RESTRICT`)
	c.Assert(stdout, qt.Contains, `DROP TYPE IF EXISTS "mood" RESTRICT`)
	c.Assert(stderr, qt.Contains, `drop schema object enum "mood"`)
	c.Assert(stderr, qt.Contains, `the server commits each drop as it runs; dropped before the failure: table "free_t"`)
	c.Assert(stderr, qt.Not(qt.Contains), "savepoint")
	c.Assert(crdbScopedCleanRelations(c, dbURL), qt.DeepEquals, []string{"keep_t"})
}

// crdbScopedCleanDatabase creates a throwaway CockroachDB database, so the
// cleanup's default schema holds only what the test creates, and returns
// the URL that connects to it.
func crdbScopedCleanDatabase(c *qt.C, suffix string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	name := fmt.Sprintf("ptah_clean_%s_%d_%d", suffix, os.Getpid(), time.Now().UnixNano()%1_000_000)
	admin, err := dbschema.ConnectToDatabase(c.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP DATABASE IF EXISTS "+name+" CASCADE")
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+name)
	c.Assert(err, qt.IsNil)

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	return parsed.String()
}

// crdbScopedCleanExec runs statements on the database dbURL names.
func crdbScopedCleanExec(c *qt.C, dbURL string, statements ...string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range statements {
		_, err = conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
}

// crdbScopedCleanRelations lists the tables and views of the database dbURL
// names, by name.
func crdbScopedCleanRelations(c *qt.C, dbURL string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), `
		SELECT table_name FROM information_schema.tables
		WHERE table_schema = current_schema()
		ORDER BY table_name`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()

	var names []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		names = append(names, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return names
}

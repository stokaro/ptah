//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// TestPostgresQuotedNameSelectorE2E inspects a PostgreSQL database holding a
// table whose stored name is "q", quote characters included, beside a table
// stored as q, and excludes one of them by a selector. PostgreSQL states no
// unquoted fold, so a selector meets a stored name as written: "q" and its
// qualified form app2."""q""" leave the table stored with quotes, and q leaves
// the other one. Unquoting the stored name first lets q leave both tables and
// "q" leave neither. A diff of the database against itself excludes the same
// table from the converted side, so it plans nothing.
func TestPostgresQuotedNameSelectorE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	native := filepath.Join(c.TempDir(), "ptah")
	buildPtah(c, ctx, e2eRepoRoot(t), native)
	dbURL := quotedNameDatabase(c, ctx, dbtarget.URL(t, dbtarget.PostgreSQL), []string{
		`CREATE SCHEMA app2`,
		`CREATE TABLE app2."""q""" (id int PRIMARY KEY)`,
		`CREATE TABLE app2.q (id int PRIMARY KEY)`,
		`CREATE TABLE app2.keep_me (id int PRIMARY KEY)`,
	})

	tests := []struct {
		exclude string
		// tables are the CREATE TABLE targets the inspection writes.
		tables []string
	}{
		{exclude: `"q"`, tables: []string{`"app2"."keep_me"`, `"app2"."q"`}},
		{exclude: `app2."""q"""`, tables: []string{`"app2"."keep_me"`, `"app2"."q"`}},
		{exclude: "q", tables: []string{`"app2"."""q"""`, `"app2"."keep_me"`}},
	}
	created := regexp.MustCompile(`(?m)^CREATE TABLE (\S+) \(`)
	for _, test := range tests {
		t.Run(test.exclude, func(t *testing.T) {
			c := qt.New(t)

			inspected, stderr, err := runCLIProcess(ctx, c.TempDir(), native,
				"schema", "inspect", "--db-url", dbURL, "--format", "sql", "--exclude", test.exclude)
			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			var tables []string
			for _, match := range created.FindAllStringSubmatch(inspected, -1) {
				tables = append(tables, match[1])
			}
			c.Assert(tables, qt.DeepEquals, test.tables, qt.Commentf("inspected:\n%s", inspected))

			diffed, stderr, err := runCLIProcess(ctx, c.TempDir(), native,
				"schema", "diff", "--from", dbURL, "--to", dbURL, "--exclude", test.exclude)
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", diffed, stderr))
			c.Assert(diffed, qt.Contains, "Schemas are synced")
			c.Assert(stderr, qt.Not(qt.Contains), "matched no objects")
		})
	}
}

// quotedNameDatabase creates a throwaway PostgreSQL database, runs setup in it,
// and returns its URL.
func quotedNameDatabase(c *qt.C, ctx context.Context, adminURL string, setup []string) string {
	c.Helper()
	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })
	name := fmt.Sprintf("ptah_quoted_selector_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })
	databaseURL := replaceDatabaseName(c, adminURL, name)
	db, err := sql.Open("pgx", databaseURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()
	for _, statement := range setup {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("setup: %s", statement))
	}
	return databaseURL
}

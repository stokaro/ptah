//go:build integration

package seeder_test

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/seeder"
)

// A protected entry is read as a table reference, so a quoted part names
// what its bare spelling does. The server decides two of the inputs: the
// schema a connection reads as its own, which a pinned search_path chooses,
// and the tables that schema holds. Each test seeds a scratch database whose
// public schema holds extra, and reads the seed table back.

// spellingTarget is a scratch PostgreSQL database with the schemas public,
// "tenant.one" and "App2", and the table public.extra.
type spellingTarget struct {
	url string
}

func newSpellingTarget(c *qt.C) spellingTarget {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.PostgreSQL)
	name := fmt.Sprintf("ptah_seed_spelling_%d", time.Now().UnixNano())
	spellingExec(c, adminURL, "CREATE DATABASE "+name)
	c.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		conn, err := dbschema.ConnectToDatabase(ctx, adminURL)
		c.Assert(err, qt.IsNil)
		defer dbschema.CloseAndWarn(conn)
		_, err = conn.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		c.Check(err, qt.IsNil)
	})
	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	target := spellingTarget{url: parsed.String()}
	for _, statement := range []string{
		`CREATE SCHEMA "tenant.one"`,
		`CREATE SCHEMA "App2"`,
		"CREATE TABLE public.extra (id integer)",
	} {
		spellingExec(c, target.url, statement)
	}
	return target
}

// searchPath is the target's URL with the search_path pinned to schema, a
// quoted identifier.
func (t spellingTarget) searchPath(c *qt.C, schema string) string {
	c.Helper()
	parsed, err := url.Parse(t.url)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", `"`+schema+`"`)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func spellingExec(c *qt.C, target, statement string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
}

// spellingSeeds is one seed that creates the table seeded in the connection's
// schema.
var spellingSeeds = fstest.MapFS{
	"010_marker.all.sql": {Data: []byte("CREATE TABLE seeded (id integer);")},
}

// spellingSeeded counts the tables named seeded in schema: 1 when the seed ran
// there, 0 when it did not.
func spellingSeeded(c *qt.C, target, schema string) int {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var tables int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = $1 AND tablename = 'seeded'", schema).Scan(&tables), qt.IsNil)
	return tables
}

// TestApplyProtectedEntrySpellingsLive_HappyPath seeds through entries that
// name a table the connection's schema does not hold, its schema spelled in
// quotes. Read by the first dot, `"tenant.one".absent` named the schema
// `"tenant` and `"public".absent` the schema `"public"`, and each refused a
// run no table fenced.
func TestApplyProtectedEntrySpellingsLive_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		schema     string
		entry      string
	}{
		{name: "the schema public, quoted", schema: "public", entry: `"public".absent`},
		{name: "a schema whose name holds a dot", schema: "tenant.one", entry: `"tenant.one".extra`},
		{name: "a schema whose name has capitals", schema: "App2", entry: `"App2".extra`},
		{
			// Bare, App2 is app2 on PostgreSQL, which holds no extra; the table
			// of its case twin "App2" is not the one the entry names.
			name:       "a bare schema in another case",
			statements: []string{"CREATE SCHEMA app2", `CREATE TABLE "App2".extra (id integer)`},
			schema:     "app2",
			entry:      "App2.extra",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newSpellingTarget(c)
			for _, statement := range test.statements {
				spellingExec(c, target.url, statement)
			}
			connection := target.searchPath(c, test.schema)
			conn, err := dbschema.ConnectToDatabase(c.Context(), connection)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			_, err = seeder.Apply(c.Context(), conn, spellingSeeds, seeder.Options{Env: "test", ProtectedTables: []string{test.entry}})

			c.Assert(err, qt.IsNil)
			c.Assert(spellingSeeded(c, target.url, test.schema), qt.Equals, 1)
		})
	}
}

// TestApplyProtectedEntrySpellingsLive_FailurePath refuses before a seed is
// written: an entry naming public.extra with a quoted part fences it as the
// bare spelling does. Matched as written, `public."extra"` fenced nothing and
// the run seeded a target that holds the table.
func TestApplyProtectedEntrySpellingsLive_FailurePath(t *testing.T) {
	for _, entry := range []string{`public."extra"`, `"public"."extra"`, `"extra"`} {
		t.Run(entry, func(t *testing.T) {
			c := qt.New(t)
			target := newSpellingTarget(c)
			conn, err := dbschema.ConnectToDatabase(c.Context(), target.url)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			result, err := seeder.Apply(c.Context(), conn, spellingSeeds, seeder.Options{Env: "test", ProtectedTables: []string{entry}})

			c.Assert(err, qt.ErrorMatches, `refusing to seed target database because protected tables exist: `+
				regexp.QuoteMeta(entry)+`; pass --allow-prod to override`)
			c.Assert(result, qt.IsNil)
			c.Assert(spellingSeeded(c, target.url, "public"), qt.Equals, 0)
		})
	}
}

// TestApplyProtectedEntryCaseTwinsLive_FailurePath refuses an entry whose
// quoted schema differs from the connection's only in case. PostgreSQL reads a
// quoted name exactly, so each entry names the case twin of the connection's
// schema, which holds the table. Compared case-insensitively, the entry read
// as the connection's own schema, the fence found no such table there, and the
// run seeded.
func TestApplyProtectedEntryCaseTwinsLive_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		statements []string
		schema     string
		entry      string
	}{
		{
			name:       "a capitalized twin of the connection's schema",
			statements: []string{"CREATE SCHEMA app2", `CREATE TABLE "App2".extra (id integer)`},
			schema:     "app2",
			entry:      `"App2".extra`,
		},
		{
			name:       "a capitalized twin of the connection's schema, after a space",
			statements: []string{"CREATE SCHEMA app2", `CREATE TABLE "App2".extra (id integer)`},
			schema:     "app2",
			entry:      `  "App2".extra`,
		},
		{
			name:       "a lower-case twin of the connection's schema",
			statements: []string{`CREATE SCHEMA "Tw"`, "CREATE SCHEMA tw", "CREATE TABLE tw.extra (id integer)"},
			schema:     "Tw",
			entry:      `"tw".extra`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newSpellingTarget(c)
			for _, statement := range test.statements {
				spellingExec(c, target.url, statement)
			}
			conn, err := dbschema.ConnectToDatabase(c.Context(), target.searchPath(c, test.schema))
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)

			result, err := seeder.Apply(c.Context(), conn, spellingSeeds, seeder.Options{Env: "test", ProtectedTables: []string{test.entry}})

			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`refusing to seed target database: the protected table entry `+
				strconv.Quote(strings.TrimSpace(test.entry))+` names a schema other than "`+test.schema+`"`)+`.*`)
			c.Assert(result, qt.IsNil)
			c.Assert(spellingSeeded(c, target.url, test.schema), qt.Equals, 0)
		})
	}
}

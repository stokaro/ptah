//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/internal/dbtarget"
)

// CockroachDB stores a routine in forms of its own. Measured on v26.3.2:
//
//   - pg_get_function_result leaves SETOF out, `items` for `RETURNS SETOF
//     public.items` and `record` for `RETURNS TABLE (...)`, while proretset
//     says the function returns a set;
//   - a body is rewritten when it is stored, the star expanded and every
//     relation qualified with the database: `SELECT * FROM public.items` is
//     stored as `SELECT public.items.id, public.items.title FROM
//     f1.public.items;`, and a PL/pgSQL body the same way.
//
// Compared as declared, every such function was dropped or replaced on every
// plan (stokaro/ptah#4058). The objects are created with plain SQL, so the
// tests measure the comparison alone.

// cockroachRoutineSpelling declares one function of each shape the server
// rewrites.
const cockroachRoutineSpelling = `CREATE TABLE public.items (id INT8 PRIMARY KEY, title TEXT);
CREATE FUNCTION public.all_items() RETURNS SETOF public.items LANGUAGE SQL STABLE AS $$ SELECT * FROM public.items $$;
CREATE FUNCTION public.item_count() RETURNS INT8 LANGUAGE SQL STABLE AS $$ SELECT count(*) FROM public.items $$;
CREATE FUNCTION public.plp() RETURNS INT8 LANGUAGE plpgsql STABLE AS $$ BEGIN RETURN (SELECT count(*) FROM public.items); END $$;
CREATE FUNCTION public.tab() RETURNS TABLE (a INT8, b TEXT) LANGUAGE SQL STABLE AS $$ SELECT id, title FROM items $$;`

// TestCockroachDBRoutinesConvergeE2E_HappyPath compares the declaration with
// the database it created, on both binaries, and finds nothing to do. The
// probe that asked the server leaves no routine behind.
func TestCockroachDBRoutinesConvergeE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
	defer cancel()
	dbURL, db := newCockroachRoutineSpellingDatabase(c, ctx, "target")
	devURL, _ := newCockroachRoutineSpellingDatabase(c, ctx, "dev")
	execCockroachStatements(c, ctx, db, cockroachRoutineSpelling)
	schema := writeCockroachRoutineSchema(c, cockroachRoutineSpelling)

	native, err := runPtahNativeWithError("schema", "apply", "--db-url", dbURL, "--schema-file", schema, "--dry-run")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", native))
	c.Assert(native, qt.Contains, "Schema is synced")

	compat, err := runCompatVerb("schema", "diff", "--from", dbURL, "--to", "file://"+schema, "--dev-url", devURL)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", compat))
	c.Assert(compat, qt.Contains, "Schemas are synced")

	var probes int
	c.Assert(db.QueryRowContext(ctx, `SELECT count(*) FROM pg_proc WHERE proname LIKE 'ptah\_routine\_probe%'`).Scan(&probes), qt.IsNil)
	c.Assert(probes, qt.Equals, 0)
}

// TestCockroachDBRoutinesStillChangeE2E_HappyPath is the control: a declaration
// that changed is still planned. A body that changed is replaced, a function
// that stops returning a set is dropped and created, and a function selecting
// `*` from a table the plan adds a column to is replaced, because its stored
// expansion lists the columns the table had.
func TestCockroachDBRoutinesStillChangeE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		want     []string
	}{
		{
			name:     "a changed body",
			declared: strings.Replace(cockroachRoutineSpelling, "SELECT count(*) FROM public.items $$", "SELECT count(*) + 1 FROM public.items $$", 1),
			want:     []string{"-- Modify function public.item_count: body"},
		},
		{
			name:     "a set that becomes a row",
			declared: strings.Replace(cockroachRoutineSpelling, "RETURNS SETOF public.items", "RETURNS public.items", 1),
			want:     []string{"-- Drop function public.all_items to recreate it: its return type changed"},
		},
		{
			name:     "a column added under a star",
			declared: strings.Replace(cockroachRoutineSpelling, "title TEXT);", "title TEXT, price INT8);", 1),
			want:     []string{`ADD COLUMN "price"`, "-- Modify function public.all_items: body"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 5*time.Minute)
			defer cancel()
			dbURL, db := newCockroachRoutineSpellingDatabase(c, ctx, "changed")
			execCockroachStatements(c, ctx, db, cockroachRoutineSpelling)

			out, err := runPtahNativeWithError("schema", "apply", "--db-url", dbURL,
				"--schema-file", writeCockroachRoutineSchema(c, test.declared), "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			for _, want := range test.want {
				c.Assert(out, qt.Contains, want)
			}
			c.Assert(out, qt.Not(qt.Contains), "-- Modify function public.plp")
			c.Assert(out, qt.Not(qt.Contains), "-- Modify function public.tab")
		})
	}
}

// newCockroachRoutineSpellingDatabase creates an empty database of its own on
// the CockroachDB test server and returns its URL with a connection to it.
func newCockroachRoutineSpellingDatabase(c *qt.C, ctx context.Context, role string) (string, *sql.DB) {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_routine_%s_%d", role, time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })

	dbURL := replaceDatabaseName(c, adminURL, name)
	db, err := sql.Open("pgx", postgresFamilyDriverURL(c, dbURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	return dbURL, db
}

// execCockroachStatements runs each statement of script, which separates them
// with a semicolon at the end of a line.
func execCockroachStatements(c *qt.C, ctx context.Context, db *sql.DB, script string) {
	c.Helper()
	for statement := range strings.SplitSeq(script, ";\n") {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
	}
}

// writeCockroachRoutineSchema writes script to a schema file of its own.
func writeCockroachRoutineSchema(c *qt.C, script string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(script+"\n"), 0o600), qt.IsNil)
	return path
}

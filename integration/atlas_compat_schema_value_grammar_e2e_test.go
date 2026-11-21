//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the pgx driver for database/sql

	"ptah.run/config/projectconfig"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/envbool/envbooltest"
)

// TestAtlasCompatSchemaValueGrammarE2E pins how ptah-compat turns a --schema
// value, and each element of `env { schemas }`, into schema names: as one line
// of comma-separated fields, the grammar of the string slice the pinned
// community binary declares the flag as.
//
// Measured on the pinned binary against a database holding schemas `one`, `a`,
// `b`, `a,b` and `public`, each with one table:
//
//	--schema '"a,b"'                 -> a,b
//	--schema 'a,b'                   -> a, b
//	--schema '"one"'                 -> one
//	env { schemas = ["\"a,b\""] }    -> a,b
//	env { schemas = ["a,b"] }        -> a, b
//	env { schemas = ["\"one\""] }    -> one
//
// The `a,b` schema is what makes the quoted rows discriminating. A reader that
// splits on every comma describes `a` and `b` for `"a,b"`, and one that does not
// unquote finds no schema called `"one"` and describes nothing.
func TestAtlasCompatSchemaValueGrammarE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	tests := []struct {
		name        string
		attribute   string
		flagSchemas []string
		wantSchemas []string
	}{
		{
			name:        "a quoted flag value names one schema that holds a comma",
			flagSchemas: []string{`"a,b"`},
			wantSchemas: []string{"a,b"},
		},
		{
			name:        "an unquoted flag value names one schema per field",
			flagSchemas: []string{"a,b"},
			wantSchemas: []string{"a", "b"},
		},
		{
			name:        "a quoted flag value is unquoted",
			flagSchemas: []string{`"one"`},
			wantSchemas: []string{"one"},
		},
		{
			name:        "a quoted configured value names one schema that holds a comma",
			attribute:   `schemas = ["\"a,b\""]`,
			wantSchemas: []string{"a,b"},
		},
		{
			name:        "an unquoted configured value names one schema per field",
			attribute:   `schemas = ["a,b"]`,
			wantSchemas: []string{"a", "b"},
		},
		{
			name:        "a quoted configured value is unquoted",
			attribute:   `schemas = ["\"one\""]`,
			wantSchemas: []string{"one"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(projectconfig.IgnoreEnvSchemasEnvVar)(t)
			sourceURL := newAtlasCompatSchemaGrammarDatabase(c, adminURL)
			configPath := writeAtlasCompatEnvSchemasConfig(c, sourceURL, test.attribute)

			rendered := runAtlasCompatEnvSchemasInspect(c, configPath, test.flagSchemas)

			c.Check(atlasCompatRenderedSchemaNames(rendered), qt.ContentEquals, test.wantSchemas,
				qt.Commentf("rendered:\n%s", rendered),
			)
		})
	}
}

// newAtlasCompatSchemaGrammarDatabase creates a throwaway database holding one
// table in each of `one`, `a`, `b`, `a,b` and `public`, and returns its URL.
func newAtlasCompatSchemaGrammarDatabase(c *qt.C, adminURL string) string {
	c.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	c.Cleanup(cancel)

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	name := fmt.Sprintf("ptah_schema_grammar_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	sourceURL := replaceDatabaseName(c, adminURL, name)
	db, err := sql.Open("pgx", sourceURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	seed := []string{
		`CREATE SCHEMA one`,
		`CREATE SCHEMA a`,
		`CREATE SCHEMA b`,
		`CREATE SCHEMA "a,b"`,
		`CREATE TABLE one.t_one (id integer PRIMARY KEY)`,
		`CREATE TABLE a.t_a (id integer PRIMARY KEY)`,
		`CREATE TABLE b.t_b (id integer PRIMARY KEY)`,
		`CREATE TABLE "a,b".t_ab (id integer PRIMARY KEY)`,
		`CREATE TABLE public.t_public (id integer PRIMARY KEY)`,
	}
	for _, statement := range seed {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("seed: %s", statement))
	}

	var seeded int
	err = db.QueryRowContext(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema IN ('one', 'a', 'b', 'a,b', 'public')`,
	).Scan(&seeded)
	c.Assert(err, qt.IsNil)
	c.Assert(seeded, qt.Equals, 5)

	return sourceURL
}

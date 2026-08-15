//go:build integration

// Read-scope guard for stokaro/ptah#1276.
//
// The issue's thesis is that Ptah's READ path and its COMPARE path set scope
// independently, and that the comparator then reads whatever the reader failed
// to look at as intent. The boundary guard beside this file crosses that
// boundary for one verb, `schema apply`, in one direction. This file crosses it
// for `schema diff` in BOTH directions, because the two directions fail
// differently and only one of them is destructive:
//
//   - a document as the DESIRED state against a database read too narrowly
//     plans a creation for an object that already exists, and the migration
//     fails;
//   - the same document as the CURRENT state plans a REMOVAL of an object
//     nothing asked to remove, and the migration succeeds.
//
// Measured on PostgreSQL 17.10 before internal/schemascope.ReadNames existed,
// against a database holding `public.a` and `extra.b` reached by a plain URL,
// compared against its own `ptah-compat schema inspect` output:
//
//	schema diff --from <plain URL> --to file://<document>
//	  CREATE SCHEMA IF NOT EXISTS extra;
//	  CREATE TABLE "extra"."b" ( "id" integer PRIMARY KEY NOT NULL );
//	schema diff --from file://<document> --to <plain URL>
//	  ALTER TABLE "extra"."b" DROP CONSTRAINT IF EXISTS "b_pkey";
//	  DROP TABLE IF EXISTS "extra"."b" CASCADE;
//
// The pinned Atlas community binary v1.3.0 answers `Schemas are synced, no
// changes to be made.` to both, at exit 0.
//
// The plain URL is the fixture and not a default: it is the URL form that hid
// #1257 and #1275, and a suite that always pins a schema cannot fail on it.
//
// The second test is the control that keeps the fix from becoming a worse
// defect. A desired state that genuinely describes one schema of a two-schema
// database still removes what it does not describe -- the goal is to stop
// UNOWNED silence from becoming a removal, not to stop removals -- and a fix
// that made an unnamed schema globally unmanaged would turn it red while
// leaving the two directions above it green.

package gonative_test

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/atlasreport"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// readScopeSeed is the two-schema database both tests below are measured on.
var readScopeSeed = []string{
	"CREATE SCHEMA extra",
	"CREATE TABLE public.a (id integer PRIMARY KEY)",
	"CREATE TABLE extra.b (id integer PRIMARY KEY)",
}

// readScopePublicOnly is a hand-authored desired state describing exactly one
// schema of that database. It carries no `ptah:not-described` record, so its
// silence about `extra` is authoritative and the removal it implies is intent.
const readScopePublicOnly = `schema "public" {
}

table "a" {
  schema = schema.public
  column "id" {
    null = false
    type = integer
  }
  primary_key {
    columns = [column.id]
  }
}
`

func TestPostgreSQLDiffAgainstTheDatabasesOwnDescriptionPlansNothingIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)

	tests := []struct {
		name string
		// sides orders the two states: which one is --from and which is --to.
		// Both orders are measured because a scope the reader missed fails
		// differently in each, and only one of the two failures is destructive.
		sides func(sourceURL, documentURL string) (from, to []string)
	}{
		{
			name: "the description as the desired state",
			sides: func(sourceURL, documentURL string) ([]string, []string) {
				return []string{sourceURL}, []string{documentURL}
			},
		},
		{
			name: "the description as the current state",
			sides: func(sourceURL, documentURL string) ([]string, []string) {
				return []string{documentURL}, []string{sourceURL}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sourceURL, devURL := readScopeDatabases(c, dsn)
			documentURL := readScopeDocumentURL(c, readScopeInspected(c, sourceURL))

			from, to := test.sides(sourceURL, documentURL)
			c.Assert(readScopePlan(c, devURL, from, to), qt.DeepEquals, []string(nil))
		})
	}
}

func TestPostgreSQLDiffAgainstADescribedScopeStillRemovesWhatItOmitsIntegration(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)

	c := qt.New(t)

	sourceURL, devURL := readScopeDatabases(c, dsn)
	documentURL := readScopeDocumentURL(c, readScopePublicOnly)

	c.Assert(
		readScopePlan(c, devURL, []string{sourceURL}, []string{documentURL}),
		qt.DeepEquals,
		[]string{
			`ALTER TABLE "extra"."b" DROP CONSTRAINT IF EXISTS "b_pkey"`,
			`DROP TABLE IF EXISTS "extra"."b" CASCADE`,
		},
	)
}

// readScopeDatabases provisions the seeded two-schema database a test measures
// and the scratch database the comparison normalizes through. Each test owns
// its own pair, because the server is shared with other suites and other
// agents and one comparison must not be able to see another's objects.
func readScopeDatabases(c *qt.C, dsn string) (sourceURL, devURL string) {
	c.Helper()

	sourceURL = newReadScopeDatabase(c, dsn, "src", readScopeSeed)
	devURL = newReadScopeDatabase(c, dsn, "dev", nil)
	return sourceURL, devURL
}

// readScopePlan compares the two states through the same entry point
// cmd/atlas/schema_diff.go and cmd/schema/diff.go both call, and returns the
// statements it planned.
func readScopePlan(c *qt.C, devURL string, from, to []string) []string {
	c.Helper()

	diff, err := atlasschema.Diff(c.Context(), atlasschema.DiffOptions{
		FromURLs: from,
		ToURLs:   to,
		DevURL:   devURL,
		// One caller's document is a compatibility projection, so the loader
		// has to accept the same names `ptah-compat` accepts. The comparison is
		// the measurement, not the parse.
		IgnoreUnknownHCLNames: true,
		Diagnostics:           io.Discard,
	})
	c.Assert(err, qt.IsNil)
	return boundaryStripComments(readScopeStatements(diff.Changes))
}

// readScopeInspected is the database's own description, rendered by the
// compatibility surface. That surface is the one whose document declares
// coverage limits, so a test using it exercises the record as well as the scope.
func readScopeInspected(c *qt.C, sourceURL string) string {
	c.Helper()

	rendered, err := atlasschema.InspectSource(c.Context(), atlasschema.InspectSourceOptions{
		URL:                    sourceURL,
		Format:                 "hcl",
		Diagnostics:            io.Discard,
		OmitAtlasRefusedBlocks: true,
		IgnoreUnknownHCLNames:  true,
	})
	c.Assert(err, qt.IsNil)
	return rendered
}

func readScopeStatements(changes []atlasreport.SchemaDiffChange) []string {
	statements := make([]string, 0, len(changes))
	for _, change := range changes {
		statements = append(statements, change.Cmd)
	}
	return statements
}

func readScopeDocumentURL(c *qt.C, document string) string {
	c.Helper()

	path := filepath.Join(c.TempDir(), "desired.hcl")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return "file://" + path
}

// newReadScopeDatabase creates and seeds a throwaway database and returns its
// plain URL: the DSN's own parameters and nothing else, because the parameter
// this suite is about is the one that is absent.
//
// The server is shared with other suites and other agents, so nothing here
// touches the database POSTGRES_TEST_DSN names.
func newReadScopeDatabase(c *qt.C, dsn, role string, seed []string) string {
	c.Helper()

	admin, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	c.Assert(admin.PingContext(c.Context()), qt.IsNil)

	name := fmt.Sprintf("ptah_readscope_%s_%d", role, time.Now().UnixNano())
	ident := pgx.Identifier{name}.Sanitize()
	_, err = admin.ExecContext(c.Context(), "CREATE DATABASE "+ident)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(
			context.WithoutCancel(c.Context()),
			"DROP DATABASE IF EXISTS "+ident+" WITH (FORCE)",
		)
		c.Check(dropErr, qt.IsNil)
		c.Check(admin.Close(), qt.IsNil)
	})

	parsed, err := url.Parse(dsn)
	c.Assert(err, qt.IsNil)
	parsed.Path = "/" + name
	parsed.RawPath = ""
	dbURL := parsed.String()
	boundarySeed(c, dbURL, seed)
	return dbURL
}

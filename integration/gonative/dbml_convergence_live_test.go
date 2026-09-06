//go:build integration

package gonative_test

import (
	"bytes"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/cmd/root"
	"ptah.run/internal/dbtarget"
)

// liveDBMLDocument is the schema this file applies, written in DBML.
const liveDBMLDocument = `Table dbml_authors {
  id BIGINT [pk]
  email VARCHAR(255) [not null, unique]
  bio TEXT
}

Table dbml_books {
  id BIGINT [pk]
  author_id BIGINT [not null]
  title VARCHAR(255) [not null]
}

Ref dbml_books_author_fk: dbml_books.author_id > dbml_authors.id
`

// liveSQLDocument is the same schema, written in a format read by a different
// code path. The type spellings and the constraint name match on purpose: a
// difference in either would make the agreement test fail for a spelling rather
// than for a meaning.
const liveSQLDocument = `CREATE TABLE dbml_authors (
  id BIGINT PRIMARY KEY,
  email VARCHAR(255) NOT NULL UNIQUE,
  bio TEXT
);
CREATE TABLE dbml_books (
  id BIGINT PRIMARY KEY,
  author_id BIGINT NOT NULL,
  title VARCHAR(255) NOT NULL,
  CONSTRAINT dbml_books_author_fk FOREIGN KEY (author_id) REFERENCES dbml_authors(id)
);
`

// TestDBMLConvergesOnLivePostgreSQLIntegration is the server half of slice 7 of
// stokaro/ptah#2065. The SQLite half runs without a container and proves the
// same two claims; this one proves them against a real engine, where the
// catalog Ptah reads back is the server's own rather than a file's.
//
// Two claims, and the second is what the first cannot make. Applying a document
// and comparing against it uses one reader for both halves, so a property the
// reader drops is missing from the database AND from the desired state and the
// two agree. Comparing against the same schema written in SQL is what catches
// that, and on SQLite it caught a real one: the parser inferred NOT NULL from
// `pk`, so one primary key described two different columns.
func TestDBMLConvergesOnLivePostgreSQLIntegration(t *testing.T) {
	c := qt.New(t)
	url := dbtarget.URL(t, dbtarget.PostgreSQL)
	dsn := requireReachableEngine(t, dbtarget.PostgreSQL, "pgx", "PostgreSQL")
	dir := t.TempDir()
	dbmlPath := filepath.Join(dir, "schema.dbml")
	sqlPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(dbmlPath, []byte(liveDBMLDocument), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(sqlPath, []byte(liveSQLDocument), 0o600), qt.IsNil)
	c.Cleanup(func() { dropLiveDBMLTables(c, dsn) })

	runNativePtah(c, "schema", "apply", "--db-url", url, "--schema-file", dbmlPath, "--auto-approve")

	converged := runNativePtah(c, "schema", "compare", "--db-url", url, "--schema-file", dbmlPath)
	agreed := runNativePtah(c, "schema", "compare", "--db-url", url, "--schema-file", sqlPath)

	c.Assert(converged, qt.Contains, "No schema differences detected")
	c.Assert(agreed, qt.Contains, "No schema differences detected",
		qt.Commentf("the DBML database and the same schema written in SQL disagree"))
}

// TestTheLiveComparisonCanReportADifferenceIntegration is the control.
//
// Both assertions above are satisfied by a comparison that reported no
// differences because it read no schema. This applies the document, widens it,
// and requires the comparison to say so.
func TestTheLiveComparisonCanReportADifferenceIntegration(t *testing.T) {
	c := qt.New(t)
	url := dbtarget.URL(t, dbtarget.PostgreSQL)
	dsn := requireReachableEngine(t, dbtarget.PostgreSQL, "pgx", "PostgreSQL")
	dir := t.TempDir()
	dbmlPath := filepath.Join(dir, "schema.dbml")
	c.Assert(os.WriteFile(dbmlPath, []byte(liveDBMLDocument), 0o600), qt.IsNil)
	c.Cleanup(func() { dropLiveDBMLTables(c, dsn) })

	runNativePtah(c, "schema", "apply", "--db-url", url, "--schema-file", dbmlPath, "--auto-approve")

	widened := filepath.Join(dir, "widened.dbml")
	c.Assert(os.WriteFile(widened,
		[]byte(liveDBMLDocument+"\nTable dbml_reviews {\n  id BIGINT [pk]\n}\n"), 0o600), qt.IsNil)

	compared := runNativePtah(c, "schema", "compare", "--db-url", url, "--schema-file", widened)

	c.Assert(compared, qt.Contains, "dbml_reviews",
		qt.Commentf("a widened document produced no difference, so the empty comparisons proved nothing"))
}

// dropLiveDBMLTables removes what this file created, so a shared server is left
// as it was found.
//
// Through the driver rather than through a Ptah command: the cleanup must run
// even when the thing under test is what broke, and it is the same route the
// neighbouring live tests take. A leftover table on a shared server is not
// tidiness -- it is the next run's mystery failure.
func dropLiveDBMLTables(c *qt.C, dsn string) {
	c.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		c.Logf("cleanup could not open the database: %v", err)
		return
	}
	defer db.Close()
	if _, err := db.Exec(
		"DROP TABLE IF EXISTS dbml_reviews, dbml_books, dbml_authors CASCADE"); err != nil {
		c.Logf("cleanup did not drop the tables: %v", err)
	}
}

// runNativePtah runs one native command in-process and returns its output.
func runNativePtah(c *qt.C, args ...string) string {
	c.Helper()
	cmd, out := nativePtahCommand()
	cmd.SetArgs(args)
	err := cmd.Execute()
	c.Assert(err, qt.IsNil, qt.Commentf("%v\n%s", args, out.String()))
	return out.String()
}

// nativePtahCommand builds the command with both streams captured into one
// buffer, which is what a caller reads.
func nativePtahCommand() (*cobra.Command, *bytes.Buffer) {
	cmd := root.NewRootCommand()
	out := &bytes.Buffer{}
	cmd.SetOut(out)
	cmd.SetErr(out)
	return cmd, out
}

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

	"ptah.run/internal/dbtarget"
)

// CockroachDB names its string and byte types STRING and BYTES and reports
// them as text and bytea, and a sized STRING(n) as text with a width: measured
// on v26.3.2. A schema file declaring the columns the same statement built
// compares equal to them, and a width the file does not share is still a
// change (stokaro/ptah#4059).

// cockroachStringTable declares every spelling the server reports in a name of
// its own.
const cockroachStringTable = "CREATE TABLE t (id INT8 PRIMARY KEY, a STRING, b STRING(10), c STRING[], d BYTES, e STRING COLLATE en)"

// cockroachStringDatabase creates a database, dropped when the test ends, and
// builds t in it. It returns the URL of the database.
func cockroachStringDatabase(c *qt.C) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_string_%d", time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	database := replaceDatabaseName(c, adminURL, name)
	execPostgresFamilySQL(c, c.Context(), database, cockroachStringTable)
	return database
}

// writeCockroachStringSchema writes statement as a schema file.
func writeCockroachStringSchema(c *qt.C, statement string) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(statement+";\n"), 0o600), qt.IsNil)
	return path
}

// TestSchemaCompareFindsCockroachStringsSyncedE2E builds the table and
// compares it with a file declaring the same statement. The comparison and a
// second apply find nothing to do.
func TestSchemaCompareFindsCockroachStringsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	target := cockroachStringDatabase(c)
	schema := writeCockroachStringSchema(c, cockroachStringTable)

	compared := runPtahIn(c, "schema", "compare", "--db-url", target, "--schema-file", schema, "--exit-code")

	c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
	c.Assert(compared.Stdout, qt.Contains, "No schema differences detected.\n")

	applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(applied.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
}

// TestSchemaCompareReportsACockroachStringWidthChangeE2E is the control: a
// file widening b and dropping the width is a change for b alone, and the
// sized column reads back with its width.
func TestSchemaCompareReportsACockroachStringWidthChangeE2E(t *testing.T) {
	c := qt.New(t)
	target := cockroachStringDatabase(c)
	schema := writeCockroachStringSchema(c, strings.Replace(cockroachStringTable, "b STRING(10)", "b STRING(20)", 1))

	got := runPtahIn(c, "schema", "compare", "--db-url", target, "--schema-file", schema, "--exit-code")

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "-- Modify column t.b: type: STRING(10) -> STRING(20)\n")
	for _, column := range []string{"t.a", "t.c", "t.d", "t.e"} {
		c.Assert(got.Stdout, qt.Not(qt.Contains), "Modify column "+column+":")
	}
}

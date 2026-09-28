//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
)

// CockroachDB builds a column declared INT or INTEGER with the width the
// session's default_int_size names, 8 unless the session sets it. A schema file
// declaring the columns the same statement built compares equal to them, in a
// session at either width, and a table built at one width is still a change
// through a session at the other (stokaro/ptah#3922).

// cockroachIntegerTable is the statement both sides are built from: every
// integer spelling, with and without a width.
const cockroachIntegerTable = "CREATE TABLE t (id bigint PRIMARY KEY, n integer, m int4, k int, s int2, s8 int8, sm smallint, b bigint)"

// cockroachIntegerDatabase creates a database, dropped when the test ends, and
// builds t in it through a session carrying options. It returns the URL of the
// database, with no session options of its own.
func cockroachIntegerDatabase(c *qt.C, options string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, dbtarget.CockroachDB)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_intwidth_%d", time.Now().UnixNano())
	createE2EDatabase(c, c.Context(), admin, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), admin, name) })
	database := replaceDatabaseName(c, adminURL, name)
	execPostgresFamilySQL(c, c.Context(), withSessionOptions(c, database, options), cockroachIntegerTable)
	return database
}

// withSessionOptions sets the PostgreSQL startup options on a URL, which is
// how a session variable reaches CockroachDB before the first statement.
//
// The space is written %20. A connection URL follows libpq, which reads `+` as
// itself rather than as the space a form encoding means by it, so a session
// given `-c+default_int_size%3D4` keeps the default width.
func withSessionOptions(c *qt.C, rawURL, options string) string {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Del("options")
	parsed.RawQuery = strings.Join([]string{
		"options=" + strings.ReplaceAll(url.QueryEscape(options), "+", "%20"),
		query.Encode(),
	}, "&")
	return parsed.String()
}

// writeCockroachIntegerSchema writes the statement as a schema file.
func writeCockroachIntegerSchema(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(cockroachIntegerTable+";\n"), 0o600), qt.IsNil)
	return path
}

// TestSchemaCompareFindsACockroachIntegerSyncedE2E builds the table and
// compares it, through a session at the width it was built with, with a file
// declaring the same statement. Both the comparison and a second apply find
// nothing to do. The default row sets an unrelated option, so both rows reach
// the server the same way and the width is the only difference.
func TestSchemaCompareFindsACockroachIntegerSyncedE2E(t *testing.T) {
	tests := []struct {
		name    string
		options string
	}{
		{name: "the default width", options: "-c application_name=ptah_intwidth"},
		{name: "a session at four bytes", options: "-c default_int_size=4"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := withSessionOptions(c, cockroachIntegerDatabase(c, test.options), test.options)
			schema := writeCockroachIntegerSchema(c)

			compared := runPtahIn(c, "schema", "compare", "--db-url", target, "--schema-file", schema, "--exit-code")

			c.Assert(compared.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", compared.Stdout, compared.Stderr))
			c.Assert(compared.Stdout, qt.Contains, "No schema differences detected.\n")

			applied := runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
			c.Assert(applied.Stdout, qt.Equals, "Schema is synced, no changes to be made.\n")
		})
	}
}

// TestSchemaCompareReportsACockroachIntegerWidthChangeE2E is the control: the
// table built at the default width, compared through a session at four bytes,
// where the same declaration builds INT4. The two columns without a width are
// a change, and the ones with a width are not.
func TestSchemaCompareReportsACockroachIntegerWidthChangeE2E(t *testing.T) {
	c := qt.New(t)
	database := cockroachIntegerDatabase(c, "-c application_name=ptah_intwidth")
	schema := writeCockroachIntegerSchema(c)

	got := runPtahIn(c, "schema", "compare", "--db-url", withSessionOptions(c, database, "-c default_int_size=4"),
		"--schema-file", schema, "--exit-code")

	c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stdout, qt.Contains, "-- Modify column t.k: type: int8 -> int4\n")
	c.Assert(got.Stdout, qt.Contains, "-- Modify column t.n: type: int8 -> int4\n")
	c.Assert(got.Stdout, qt.Not(qt.Contains), "Modify column t.m")
	c.Assert(got.Stdout, qt.Not(qt.Contains), "Modify column t.s8")
}

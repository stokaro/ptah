//go:build integration

package integration_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/root"
)

// runPtahNativeOutcome runs one native command in-process and returns its
// combined output and its error, for a command the test expects to fail.
func runPtahNativeOutcome(args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// notNullTarget is a scratch database holding t3648 with the rows given, and
// a schema file that makes its column c NOT NULL.
type notNullTarget struct {
	url, schema string
}

func newNotNullTarget(c *qt.C, liveColumn, rows, desiredColumn string) notNullTarget {
	c.Helper()
	target, _ := scratchReplayDatabase(c)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "CREATE TABLE t3648 (id bigint PRIMARY KEY, "+liveColumn+")")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(c.Context(), "INSERT INTO t3648 VALUES "+rows)
	c.Assert(err, qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t3648 (id bigint PRIMARY KEY, "+desiredColumn+");\n"), 0o600), qt.IsNil)
	return notNullTarget{url: target, schema: schema}
}

// readNotNullRows answers the rows of t3648 as id:value, NULL spelled out, and
// whether c is nullable.
func readNotNullRows(c *qt.C, url string) (rows, nullable string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	err = conn.QueryRowContext(c.Context(), `
		SELECT (SELECT string_agg(id || ':' || coalesce(c::text, 'NULL'), ' ' ORDER BY id) FROM t3648),
		       (SELECT is_nullable FROM information_schema.columns
		        WHERE table_schema = current_schema() AND table_name = 't3648' AND column_name = 'c')`,
	).Scan(&rows, &nullable)
	c.Assert(err, qt.IsNil)
	return rows, nullable
}

// A column made NOT NULL with no default, over a NULL row, is refused by the
// server and left as it was. Filled with a value its type suggests, the NULL
// becomes 0 or the empty string and the apply reports success; Atlas CE
// v1.3.0 refuses the same change with SQLSTATE 23502 (stokaro/ptah#3648).
func TestSchemaApplySetNotNullWithoutDefault_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		rows     string
		desired  string
		wantRows string
	}{
		{name: "an integer column", live: "c integer", rows: "(1, NULL), (2, 5)", desired: "c integer NOT NULL", wantRows: "1:NULL 2:5"},
		{name: "a text column", live: "c text", rows: "(1, NULL), (2, 'x')", desired: "c text NOT NULL", wantRows: "1:NULL 2:x"},
		{name: "a boolean column", live: "c boolean", rows: "(1, NULL), (2, true)", desired: "c boolean NOT NULL", wantRows: "1:NULL 2:true"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newNotNullTarget(c, test.live, test.rows, test.desired)

			plan := runPtahNative(c, "schema", "apply", "--db-url", target.url, "--schema-file", target.schema, "--dry-run")
			out, err := runPtahNativeOutcome("schema", "apply", "--db-url", target.url, "--schema-file", target.schema, "--auto-approve")

			c.Assert(plan, qt.Contains, `SET NOT NULL fails if any row of "t3648" holds NULL in "c"; the column declares no default to fill it with.`)
			c.Assert(plan, qt.Not(qt.Contains), "UPDATE")
			c.Assert(err, qt.ErrorMatches, `(?s).*column "c" of relation "t3648" contains null values.*`, qt.Commentf("%s", out))
			rows, nullable := readNotNullRows(c, target.url)
			c.Assert(rows, qt.Equals, test.wantRows)
			c.Assert(nullable, qt.Equals, "YES")
		})
	}
}

// With no NULL row the same change applies, and a column that declares a
// default fills its NULL rows with it: the value is one the author wrote.
// Atlas CE v1.3.0 refuses the second row as it does the rows above, so there
// Ptah is deliberately more permissive, with the author's own value.
func TestSchemaApplySetNotNullWithoutDefault_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		live     string
		rows     string
		desired  string
		wantRows string
	}{
		{name: "no NULL row", live: "c integer", rows: "(1, 4), (2, 5)", desired: "c integer NOT NULL", wantRows: "1:4 2:5"},
		{name: "a declared default fills the NULL row", live: "c integer", rows: "(1, NULL), (2, 5)", desired: "c integer NOT NULL DEFAULT 9", wantRows: "1:9 2:5"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newNotNullTarget(c, test.live, test.rows, test.desired)

			runPtahNative(c, "schema", "apply", "--db-url", target.url, "--schema-file", target.schema, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target.url, "--schema-file", target.schema, "--dry-run")

			rows, nullable := readNotNullRows(c, target.url)
			c.Assert(rows, qt.Equals, test.wantRows)
			c.Assert(nullable, qt.Equals, "NO")
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// addedNotNullComment is what a plan says above a column that stops on a row.
const addedNotNullComment = `ADD COLUMN "c" fails if "t3648" holds any row; the column is NOT NULL and declares no default to fill it with.`

// readAddedColumnState answers how many rows t3648 holds, and the nullability of its
// column c, or an empty string where it has no such column.
func readAddedColumnState(c *qt.C, url string) (rows int, nullable string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	err = conn.QueryRowContext(c.Context(), `
		SELECT (SELECT count(*) FROM t3648),
		       COALESCE((SELECT is_nullable FROM information_schema.columns
		        WHERE table_schema = current_schema() AND table_name = 't3648' AND column_name = 'c'), '')`,
	).Scan(&rows, &nullable)
	c.Assert(err, qt.IsNil)
	return rows, nullable
}

// A NOT NULL column with no default, added to a table with a row, is refused by the
// server, as the plan's comment says, and the table stays as it was.
func TestSchemaApplyAddNotNullWithoutDefault_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := newNotNullTarget(c, "b integer", "(1, 4)", "b integer, c text NOT NULL")

	plan := runPtahNative(c, "schema", "apply", "--db-url", target.url, "--schema-file", target.schema, "--dry-run")
	out, err := runPtahNativeOutcome("schema", "apply", "--db-url", target.url, "--schema-file", target.schema, "--auto-approve")

	c.Assert(plan, qt.Contains, addedNotNullComment)
	c.Assert(err, qt.ErrorMatches, `(?s).*column "c" of relation "t3648" contains null values.*`, qt.Commentf("%s", out))
	rows, nullable := readAddedColumnState(c, target.url)
	c.Assert(rows, qt.Equals, 1)
	c.Assert(nullable, qt.Equals, "")
}

// The statement is valid on an empty table, so the plan writes it as it is, and the next
// plan reads synced.
func TestSchemaApplyAddNotNullWithoutDefault_HappyPath(t *testing.T) {
	c := qt.New(t)
	target := newNotNullTarget(c, "b integer", "(1, 4)", "b integer, c text NOT NULL")
	conn, err := dbschema.ConnectToDatabase(c.Context(), target.url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "TRUNCATE t3648")
	c.Assert(err, qt.IsNil)

	applied := runPtahNative(c, "schema", "apply", "--db-url", target.url, "--schema-file", target.schema, "--auto-approve")
	again := runPtahNative(c, "schema", "apply", "--db-url", target.url, "--schema-file", target.schema, "--dry-run")

	c.Assert(applied, qt.Contains, addedNotNullComment)
	rows, nullable := readAddedColumnState(c, target.url)
	c.Assert(rows, qt.Equals, 0)
	c.Assert(nullable, qt.Equals, "NO")
	c.Assert(again, qt.Contains, "Schema is synced")
}

// Each of these columns is NOT NULL and the server gives the row a value, so no plan may
// say the statement fails. They all go in one plan, on one table with a row.
func TestSchemaApplyAddNotNullTheServerFills_HappyPath(t *testing.T) {
	c := qt.New(t)
	target := newNotNullTarget(c, "b integer", "(1, 4)", "b integer")
	schema := filepath.Join(c.TempDir(), "filled.sql")
	c.Assert(os.WriteFile(schema, []byte(`CREATE DOMAIN filled AS text DEFAULT 'y';
CREATE TABLE t3648 (id bigint PRIMARY KEY, b integer,
	c text NOT NULL DEFAULT 'x',
	d timestamptz NOT NULL DEFAULT now(),
	e integer GENERATED ALWAYS AS IDENTITY,
	f serial,
	g bigint GENERATED ALWAYS AS (id + 1) STORED NOT NULL,
	h filled NOT NULL);
`), 0o600), qt.IsNil)

	applied := runPtahNative(c, "schema", "apply", "--db-url", target.url, "--schema-file", schema, "--auto-approve")

	c.Assert(applied, qt.Not(qt.Contains), "holds any row")
	rows, nullable := readAddedColumnState(c, target.url)
	c.Assert(rows, qt.Equals, 1)
	c.Assert(nullable, qt.Equals, "NO")
}

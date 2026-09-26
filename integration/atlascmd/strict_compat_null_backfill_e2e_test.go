//go:build integration

package atlas_test

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/dbtarget"
)

// strictCompat is the environment that selects the strict Community Edition
// policy for a ptah-compat process. A run given no additions has no PTAH_
// variable at all, which is the full surface.
var strictCompat = []string{"PTAH_ATLAS_STRICT_COMPAT=1"}

// nullBackfillDiff is one `migrate diff` the tests below run: a directory, the
// schema file it is diffed to, which of the written files to read, the
// database that file is run on, which holds a row whose c is NULL, and the
// statements strict mode writes.
type nullBackfillDiff struct {
	name       string
	dirFormat  []string
	files      map[string]string
	schema     string
	written    string
	target     []string
	wantStrict []string
}

// The diffs. The first two write Atlas's own layout; the third writes the
// golang-migrate layout, whose rollback half sets NOT NULL again.
var nullBackfillDiffs = []nullBackfillDiff{
	{
		name:    "NOT NULL and a default gained together",
		files:   map[string]string{"20260101000000_init.sql": "CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer);"},
		schema:  "CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer NOT NULL DEFAULT 9);",
		written: ".sql",
		target:  []string{"CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer)", "INSERT INTO t3660 VALUES (1, NULL)"},
		wantStrict: []string{
			`ALTER TABLE "t3660" ALTER COLUMN "c" SET NOT NULL`,
			`ALTER TABLE "t3660" ALTER COLUMN "c" SET DEFAULT 9`,
		},
	},
	{
		name:       "NOT NULL gained under a default the column has",
		files:      map[string]string{"20260101000000_init.sql": "CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer DEFAULT 9);"},
		schema:     "CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer NOT NULL DEFAULT 9);",
		written:    ".sql",
		target:     []string{"CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer DEFAULT 9)", "INSERT INTO t3660 VALUES (1, NULL)"},
		wantStrict: []string{`ALTER TABLE "t3660" ALTER COLUMN "c" SET NOT NULL`},
	},
	{
		name:      "the rollback of a dropped NOT NULL, in the golang-migrate layout",
		dirFormat: []string{"--dir-format", "golang-migrate"},
		files: map[string]string{
			"1_init.up.sql":   "CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer NOT NULL DEFAULT 9);",
			"1_init.down.sql": "DROP TABLE t3660;",
		},
		schema:     "CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer DEFAULT 9);",
		written:    ".down.sql",
		target:     []string{"CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer DEFAULT 9)", "INSERT INTO t3660 VALUES (1, NULL)"},
		wantStrict: []string{`ALTER TABLE "t3660" ALTER COLUMN "c" SET NOT NULL`},
	},
}

// diffNullBackfill runs the diff with a ptah-compat process under env, and
// answers the statements of the file it wrote, comments stripped.
func diffNullBackfill(c *qt.C, compat string, env []string, diff nullBackfillDiff) []string {
	c.Helper()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for name, content := range diff.files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content+"\n"), 0o600), qt.IsNil)
	}
	schema := filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(diff.schema+"\n"), 0o600), qt.IsNil)
	hash := append([]string{"migrate", "hash", "--dir", "file://" + dir}, diff.dirFormat...)
	stdout, stderr, code := runAtlasBinary(compat, env, hash...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	dev := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), nil)
	args := append([]string{"migrate", "diff", "--dir", "file://" + dir, "--to", "file://" + schema, "--dev-url", dev}, diff.dirFormat...)
	stdout, stderr, code = runAtlasBinary(compat, env, args...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	var written []string
	for _, entry := range entries {
		written = append(written, entry.Name())
	}
	written = slices.DeleteFunc(written, func(name string) bool {
		_, authored := diff.files[name]
		return authored || name == "atlas.sum" || !strings.HasSuffix(name, diff.written)
	})
	c.Assert(written, qt.HasLen, 1)
	content, err := os.ReadFile(filepath.Join(dir, written[0]))
	c.Assert(err, qt.IsNil)
	return sqlutil.SplitSQLStatementsForDialect(sqlutil.StripCommentsForDialect(string(content), platform.Postgres), platform.Postgres)
}

// runOnNullRow runs the statements on a database built from target, stops at
// the first the server refuses, and answers the row afterwards and that
// refusal.
func runOnNullRow(c *qt.C, target, statements []string) (string, error) {
	c.Helper()
	url := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), target)
	db, err := sql.Open("pgx", url)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var runErr error
	for _, statement := range statements {
		if _, runErr = db.ExecContext(context.Background(), statement); runErr != nil {
			break
		}
	}
	return nullBackfillRow(c, db), runErr
}

func nullBackfillRow(c *qt.C, db *sql.DB) string {
	c.Helper()
	var row string
	c.Assert(db.QueryRowContext(context.Background(), `SELECT coalesce(c::text, 'NULL') FROM t3660 WHERE id = 1`).Scan(&row), qt.IsNil)
	return row
}

// Under PTAH_ATLAS_STRICT_COMPAT=1 ptah-compat writes what Atlas CE v1.3.0
// writes for a column made NOT NULL that declares a default: SET NOT NULL, and
// no fill of the NULL rows before it. Measured on PostgreSQL 18 with the same
// directories and schema files, the pinned binary wrote, row by row:
//
//	ALTER TABLE "public"."t3660" ALTER COLUMN "c" SET NOT NULL, ALTER COLUMN "c" SET DEFAULT 9;
//	ALTER TABLE "public"."t3660" ALTER COLUMN "c" SET NOT NULL;
//	ALTER TABLE "public"."t3660" ALTER COLUMN "c" SET NOT NULL;  -- the .down.sql
//
// Ptah writes one statement per clause and leaves the schema unqualified; the
// statements are the same. Run on a table holding a NULL, each file fails with
// SQLSTATE 23502, as the pinned binary's does, and the row keeps its NULL.
func TestStrictCompatMigrateDiffWritesNoNullBackfill_FailurePath(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat")
	for _, diff := range nullBackfillDiffs {
		t.Run(diff.name, func(t *testing.T) {
			c := qt.New(t)

			statements := diffNullBackfill(c, compat, strictCompat, diff)
			row, runErr := runOnNullRow(c, diff.target, statements)

			c.Assert(statements, qt.DeepEquals, diff.wantStrict)
			c.Assert(runErr, qt.ErrorMatches, `(?s).*column "c" of relation "t3660" contains null values.*`)
			c.Assert(row, qt.Equals, "NULL")
		})
	}
}

// Without the variable the same diffs fill the NULL rows from the declared
// default before SET NOT NULL, and each file applies. This is the control that
// the strict rows above differ by the policy and not by the directory.
func TestStrictCompatMigrateDiffWritesNoNullBackfill_HappyPath(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat")
	for _, diff := range nullBackfillDiffs {
		t.Run(diff.name, func(t *testing.T) {
			c := qt.New(t)

			statements := diffNullBackfill(c, compat, nil, diff)
			row, runErr := runOnNullRow(c, diff.target, statements)

			c.Assert(statements, qt.Not(qt.HasLen), 0)
			c.Assert(statements[0], qt.Matches, `(?s)DO \$\$.*UPDATE "t3660" SET "c" = '?9'? WHERE "c" IS NULL.*`)
			c.Assert(statements, qt.Contains, `ALTER TABLE "t3660" ALTER COLUMN "c" SET NOT NULL`)
			c.Assert(runErr, qt.IsNil)
			c.Assert(row, qt.Equals, "9")
		})
	}
}

// `schema apply` plans through the same policy. Measured on PostgreSQL 18,
// Atlas CE v1.3.0 refused this apply with SQLSTATE 23502 and left the row
// NULL; under strict mode so does ptah-compat, and without it the NULL row is
// filled with the default and the apply completes.
func TestStrictCompatSchemaApplyWritesNoNullBackfill(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat")
	tests := []struct {
		name       string
		env        []string
		wantCode   int
		wantStderr string
		wantRow    string
	}{
		{name: "strict", env: strictCompat, wantCode: 1, wantStderr: `(?s).*column "c" of relation "t3660" contains null values.*`, wantRow: "NULL"},
		{name: "full", env: nil, wantCode: 0, wantStderr: ``, wantRow: "9"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), []string{
				"CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer)", "INSERT INTO t3660 VALUES (1, NULL)",
			})
			dev := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), nil)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer NOT NULL DEFAULT 9);\n"), 0o600), qt.IsNil)

			stdout, stderr, code := runAtlasBinary(compat, test.env,
				"schema", "apply", "--url", target, "--to", "file://"+schema, "--dev-url", dev, "--auto-approve")

			c.Assert(code, qt.Equals, test.wantCode, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
			c.Assert(stderr, qt.Matches, test.wantStderr)
			db, err := sql.Open("pgx", target)
			c.Assert(err, qt.IsNil)
			defer func() { c.Check(db.Close(), qt.IsNil) }()
			c.Assert(nullBackfillRow(c, db), qt.Equals, test.wantRow)
		})
	}
}

// `schema diff` prints the plan through the same policy: the fill's UPDATE
// under the full surface, and SET NOT NULL alone under strict mode.
func TestStrictCompatSchemaDiffWritesNoNullBackfill(t *testing.T) {
	c := qt.New(t)
	compat := buildSchemaInspectBinary(c, "ptah-compat", "ptah.run/cmd/ptah-compat")
	tests := []struct {
		name        string
		env         []string
		wantUpdates int
	}{
		{name: "strict", env: strictCompat, wantUpdates: 0},
		{name: "full", env: nil, wantUpdates: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			from := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), []string{
				"CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer)",
			})
			dev := newCleanGateDatabase(c, dbtarget.URL(c, dbtarget.PostgreSQL), nil)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte("CREATE TABLE t3660 (id bigint PRIMARY KEY, c integer NOT NULL DEFAULT 9);\n"), 0o600), qt.IsNil)

			stdout, stderr, code := runAtlasBinary(compat, test.env,
				"schema", "diff", "--from", from, "--to", "file://"+schema, "--dev-url", dev)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout=%q stderr=%q", stdout, stderr))
			c.Assert(stdout, qt.Contains, `ALTER COLUMN "c" SET NOT NULL`)
			c.Assert(strings.Count(stdout, "UPDATE"), qt.Equals, test.wantUpdates, qt.Commentf("%s", stdout))
		})
	}
}

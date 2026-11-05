//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// columnKeyRead prints the primary key of public.parent as the server does, and the
// empty string where it holds none.
const columnKeyRead = "SELECT coalesce((SELECT conname || ': ' || pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'public.parent'::regclass AND contype = 'p'), '')"

// columnKeyString runs a query that returns one string against the database at url.
func columnKeyString(c *qt.C, url, query string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var value string
	c.Assert(conn.QueryRowContext(c.Context(), query).Scan(&value), qt.IsNil, qt.Commentf("%s", query))
	return value
}

// keyedApply runs the native binary's `schema apply` of desired over the database at
// target.
func keyedApply(c *qt.C, target, desired string) clirun.Result {
	c.Helper()
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	return runPtahIn(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--schemas", "public", "--auto-approve")
}

// TestSchemaApplyColumnPrimaryKeyE2E_HappyPath declares a primary key on a column of a
// table that holds another key or none. The plan compared it only as a flag on the
// column and wrote nothing for it, so the table lost its key or never gained one, and
// the next run reported `Schema is synced`. Each row reads the key back from the
// server, which refuses a duplicate through it, and a second run finds nothing to do.
func TestSchemaApplyColumnPrimaryKeyE2E_HappyPath(t *testing.T) {
	tests := []struct{ name, built, desired, want string }{
		{
			name:    "moved to another column",
			built:   "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\nINSERT INTO parent VALUES (1, 1);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);\n",
			want:    "parent_pkey: PRIMARY KEY (code)",
		},
		{
			name:    "a table with no key gains one",
			built:   "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL);\nINSERT INTO parent VALUES (1, 1);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);\n",
			want:    "parent_pkey: PRIMARY KEY (code)",
		},
		{
			name:    "a named key keeps its name on another column",
			built:   "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pk PRIMARY KEY (id));\nINSERT INTO parent VALUES (1, 1);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);\n",
			want:    "parent_pk: PRIMARY KEY (code)",
		},
		{
			name:    "a key over two columns narrowed to the second",
			built:   "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (id, code));\nINSERT INTO parent VALUES (1, 1);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);\n",
			want:    "parent_pkey: PRIMARY KEY (code)",
		},
		{
			name:    "a key over two columns narrowed to the first",
			built:   "CREATE TABLE parent (code bigint NOT NULL, id bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (code, id));\nINSERT INTO parent VALUES (1, 1);\n",
			desired: "CREATE TABLE parent (code bigint PRIMARY KEY, id bigint NOT NULL);\n",
			want:    "parent_pkey: PRIMARY KEY (code)",
		},
		{
			name:    "a table with no key gains one over a column the plan adds",
			built:   "CREATE TABLE parent (id bigint NOT NULL);\nINSERT INTO parent VALUES (1);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigserial PRIMARY KEY);\n",
			want:    "parent_pkey: PRIMARY KEY (code)",
		},
		{
			name:    "a nullable column",
			built:   "CREATE TABLE parent (id bigint NOT NULL, code bigint);\nINSERT INTO parent VALUES (1, 1);\n",
			desired: "CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);\n",
			want:    "parent_pkey: PRIMARY KEY (code)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			target, run := runExactApply(c, test.built, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, columnKeyRead), qt.Equals, test.want)
			conn, err := dbschema.ConnectToDatabase(c.Context(), target)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			_, err = conn.ExecContext(c.Context(), "INSERT INTO parent (id, code) VALUES (2, 1)")
			c.Assert(err, qt.ErrorMatches, `(?s).*duplicate key value violates unique constraint.*`)
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

// TestSchemaApplyColumnPrimaryKeyE2E_TheHeldKeyStays is the control: the declaration
// names the key the table holds, and the first run finds nothing to do. A key that
// covers another column stays too, since a column's key cannot say anything about the
// column it covers.
func TestSchemaApplyColumnPrimaryKeyE2E_TheHeldKeyStays(t *testing.T) {
	tests := []struct{ name, built, want string }{
		{name: "the same key", built: "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\n", want: "parent_pkey: PRIMARY KEY (id)"},
		{
			name:  "a key that covers another column",
			built: "CREATE TABLE parent (id bigint NOT NULL, code bigint NOT NULL, CONSTRAINT parent_pkey PRIMARY KEY (id) INCLUDE (code));\n",
			want:  "parent_pkey: PRIMARY KEY (id) INCLUDE (code)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			target, run := runExactApply(c, test.built, "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\n")

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, "Schema is synced")
			c.Assert(columnKeyString(c, target, columnKeyRead), qt.Equals, test.want)
		})
	}
}

// TestSchemaApplyColumnPrimaryKeyE2E_ADuplicateStopsTheKey holds rows the key cannot
// cover: the server refuses the key, and the plan's one transaction takes the rest back.
func TestSchemaApplyColumnPrimaryKeyE2E_ADuplicateStopsTheKey(t *testing.T) {
	c := qt.New(t)

	target, run := runExactApply(c,
		"CREATE TABLE parent (id bigint PRIMARY KEY, code bigint);\nINSERT INTO parent VALUES (1, 1), (2, 1);\n",
		"CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);\n")

	c.Assert(run.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
	c.Assert(run.Stderr, qt.Contains, `could not create unique index "parent_pkey"`)
	c.Assert(columnKeyString(c, target, columnKeyRead), qt.Equals, "parent_pkey: PRIMARY KEY (id)")
	c.Assert(readInt(c, target, "SELECT count(*) FROM pg_attribute WHERE attrelid = 'public.parent'::regclass AND attname = 'code' AND attnotnull"), qt.Equals, 0)
}

// TestMigrationsGenerateColumnPrimaryKeyE2E_RunsBothWays runs the plan through a
// migration pair and reads each direction back.
func TestMigrationsGenerateColumnPrimaryKeyE2E_RunsBothWays(t *testing.T) {
	c := qt.New(t)
	target := databaseBuiltFrom(c, "CREATE TABLE parent (id bigint PRIMARY KEY, code bigint NOT NULL);\n")
	dir := exactGeneratedFile(c, target, "CREATE TABLE parent (id bigint NOT NULL, code bigint PRIMARY KEY);\n")

	up := runPtahIn(c, "migrations", "up", "--db-url", target, "--migrations-dir", dir, "--allow-destructive")

	c.Assert(up.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", up.Stdout, up.Stderr))
	c.Assert(columnKeyString(c, target, columnKeyRead), qt.Equals, "parent_pkey: PRIMARY KEY (code)")

	down := runPtahIn(c, "migrations", "down", "--db-url", target, "--migrations-dir", dir, "--target", "0", "--confirm")

	c.Assert(down.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", down.Stdout, down.Stderr))
	c.Assert(columnKeyString(c, target, columnKeyRead), qt.Equals, "parent_pkey: PRIMARY KEY (id)")
}

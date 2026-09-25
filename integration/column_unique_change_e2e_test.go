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

// uniqueChangeTarget is a scratch database the live schema was executed on,
// and a schema file holding the desired one.
type uniqueChangeTarget struct {
	url, schema string
}

func newUniqueChangeTarget(c *qt.C, live, desired string) uniqueChangeTarget {
	c.Helper()
	target, _ := scratchReplayDatabase(c)
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), live)
	c.Assert(err, qt.IsNil)
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(desired), 0o600), qt.IsNil)
	return uniqueChangeTarget{url: target, schema: schema}
}

// readUniqueConstraints answers the UNIQUE constraints of the current schema
// as `table:name(columns)`, in name order.
func readUniqueConstraints(c *qt.C, url string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var constraints string
	err = conn.QueryRowContext(c.Context(), `
		SELECT coalesce(string_agg(
			conrelid::regclass::text || ':' || conname || '(' ||
			(SELECT string_agg(attname, ',' ORDER BY attnum) FROM pg_attribute
			 WHERE attrelid = conrelid AND attnum = ANY (conkey)) || ')',
			' ' ORDER BY conname), '')
		FROM pg_constraint
		WHERE contype = 'u' AND connamespace = current_schema()::regnamespace`,
	).Scan(&constraints)
	c.Assert(err, qt.IsNil)
	return constraints
}

// runPtahNativeWithError runs one native command in-process and returns its
// combined output and its error, for a command the test expects to fail.
func runPtahNativeWithError(args ...string) (string, error) {
	cmd := root.NewRootCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// A column-level UNIQUE declared on an existing column is created, under the
// name the server gives it inside CREATE TABLE, and a second comparison finds
// nothing left to do. Without the plan adding it, the comparison reports
// `unique: false -> true` on every run and the column never gains the
// constraint (stokaro/ptah#3649). The loss is the control: the removed
// constraint is dropped, as it is without this change.
func TestSchemaApplyColumnUniqueChangeLive_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		live    string
		desired string
		want    string
	}{
		{
			name:    "UNIQUE gained",
			live:    "CREATE TABLE flags3649 (id bigint PRIMARY KEY, code integer); INSERT INTO flags3649 VALUES (1, 1), (2, NULL), (3, NULL);",
			desired: "CREATE TABLE flags3649 (id bigint PRIMARY KEY, code integer UNIQUE);",
			want:    "flags3649:flags3649_code_key(code)",
		},
		{
			name: "UNIQUE gained under a name past 63 bytes",
			live: "CREATE TABLE t3649_a_rather_long_table_name_for_truncation (id bigint PRIMARY KEY, a_rather_long_column_name_too integer);",
			desired: "CREATE TABLE t3649_a_rather_long_table_name_for_truncation " +
				"(id bigint PRIMARY KEY, a_rather_long_column_name_too integer UNIQUE);",
			want: "t3649_a_rather_long_table_name_for_truncation:" +
				"t3649_a_rather_long_table_nam_a_rather_long_column_name_too_key(a_rather_long_column_name_too)",
		},
		{
			name: "UNIQUE gained by a column a new foreign key references",
			live: "CREATE TABLE users3649 (id bigint PRIMARY KEY, email text);",
			desired: "CREATE TABLE users3649 (id bigint PRIMARY KEY, email text UNIQUE);\n" +
				"CREATE TABLE orders3649 (id bigint PRIMARY KEY, user_email text REFERENCES users3649 (email));",
			want: "users3649:users3649_email_key(email)",
		},
		{
			name:    "UNIQUE lost",
			live:    "CREATE TABLE flags3649 (id bigint PRIMARY KEY, code integer UNIQUE);",
			desired: "CREATE TABLE flags3649 (id bigint PRIMARY KEY, code integer);",
			want:    "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := newUniqueChangeTarget(c, test.live, test.desired)

			runPtahNative(c, "schema", "apply", "--db-url", target.url, "--schema-file", target.schema, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target.url, "--schema-file", target.schema, "--dry-run")

			c.Assert(readUniqueConstraints(c, target.url), qt.Equals, test.want)
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// Rows that repeat a value make the server refuse the constraint, and the
// column is left without it. The plan does not pretend otherwise.
func TestSchemaApplyColumnUniqueChangeLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	target := newUniqueChangeTarget(c,
		"CREATE TABLE flags3649 (id bigint PRIMARY KEY, code integer); INSERT INTO flags3649 VALUES (1, 7), (2, 7);",
		"CREATE TABLE flags3649 (id bigint PRIMARY KEY, code integer UNIQUE);",
	)

	out, err := runPtahNativeWithError("schema", "apply", "--db-url", target.url, "--schema-file", target.schema, "--auto-approve")

	c.Assert(err, qt.ErrorMatches, `(?s).*could not create unique index "flags3649_code_key".*`, qt.Commentf("%s", out))
	c.Assert(readUniqueConstraints(c, target.url), qt.Equals, "")
}

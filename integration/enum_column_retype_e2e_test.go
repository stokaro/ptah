//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSchemaApplyRetypesAColumnAcrossAnEnumLive changes the type of a column
// with a default between an enum, an array of one, a domain over one and
// text on PostgreSQL, and reads the column's default back.
//
// PostgreSQL keeps the old type inside a converted default: `'a'::status`
// becomes `('a'::status)::text`, which still depends on the enum, so DROP TYPE
// is refused, or with CASCADE drops the default. A default that cannot be
// converted makes ALTER COLUMN TYPE fail. The plan drops the default before
// the TYPE clause, casts through text, and sets the declared default after
// it, so the column holds the declared default and nothing of the old type,
// and a second comparison finds nothing to do.
func TestSchemaApplyRetypesAColumnAcrossAnEnumLive(t *testing.T) {
	const status = "CREATE TYPE status AS ENUM ('a', 'b');\n"
	tests := []struct {
		name        string
		before      string
		after       string
		wantDefault *string
	}{
		{name: "an enum to text", before: status + "CREATE TABLE t3987 (id bigint PRIMARY KEY, s status DEFAULT 'a');\n",
			after: "CREATE TABLE t3987 (id bigint PRIMARY KEY, s text DEFAULT 'a');\n", wantDefault: new("'a'::text")},
		{name: "text to an enum", before: "CREATE TABLE t3987 (id bigint PRIMARY KEY, s text DEFAULT 'a');\n",
			after: status + "CREATE TABLE t3987 (id bigint PRIMARY KEY, s status DEFAULT 'a');\n", wantDefault: new("'a'::status")},
		{name: "text with a default to an enum with none", before: "CREATE TABLE t3987 (id bigint PRIMARY KEY, s text DEFAULT 'a');\n",
			after: status + "CREATE TABLE t3987 (id bigint PRIMARY KEY, s status);\n"},
		{name: "an enum to another enum",
			before:      status + "CREATE TABLE t3987 (id bigint PRIMARY KEY, s status DEFAULT 'a');\n",
			after:       "CREATE TYPE grade AS ENUM ('a', 'b');\nCREATE TABLE t3987 (id bigint PRIMARY KEY, s grade DEFAULT 'a');\n",
			wantDefault: new("'a'::grade")},
		{name: "an array of an enum to an array of text",
			before:      status + "CREATE TABLE t3987 (id bigint PRIMARY KEY, s status[] DEFAULT '{a}');\n",
			after:       "CREATE TABLE t3987 (id bigint PRIMARY KEY, s text[] DEFAULT '{a}');\n",
			wantDefault: new("'{a}'::text[]")},
		{name: "an array of text to an array of an enum",
			before:      "CREATE TABLE t3987 (id bigint PRIMARY KEY, s text[] DEFAULT '{a}');\n",
			after:       status + "CREATE TABLE t3987 (id bigint PRIMARY KEY, s status[] DEFAULT '{a}');\n",
			wantDefault: new("'{a}'::status[]")},
		{name: "a domain over an enum to text",
			before:      status + "CREATE DOMAIN dstatus AS status;\nCREATE TABLE t3987 (id bigint PRIMARY KEY, s dstatus DEFAULT 'a');\n",
			after:       "CREATE TABLE t3987 (id bigint PRIMARY KEY, s text DEFAULT 'a');\n",
			wantDefault: new("'a'::text")},
		{name: "text to a domain over an enum",
			before:      "CREATE TABLE t3987 (id bigint PRIMARY KEY, s text DEFAULT 'a');\n",
			after:       status + "CREATE DOMAIN dstatus AS status;\nCREATE TABLE t3987 (id bigint PRIMARY KEY, s dstatus DEFAULT 'a');\n",
			wantDefault: new("'a'::status")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := scratchReplayDatabase(c)
			dir := c.TempDir()
			before := filepath.Join(dir, "before.sql")
			after := filepath.Join(dir, "after.sql")
			c.Assert(os.WriteFile(before, []byte(test.before), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(after, []byte(test.after), 0o600), qt.IsNil)
			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", before, "--auto-approve")

			runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--auto-approve")
			again := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", after, "--dry-run")

			columnDefault, _ := readColumnDefinition(c, target, "t3987", "s")
			c.Assert(columnDefault, qt.DeepEquals, test.wantDefault)
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

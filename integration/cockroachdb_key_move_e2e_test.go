//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// cockroachKeyRead prints the primary key and the unique keys of public.parent as the
// server does. A second key over the old column would show here.
const cockroachKeyRead = "SELECT string_agg(conname || ': ' || pg_get_constraintdef(oid), '; ' ORDER BY conname) " +
	"FROM pg_constraint WHERE conrelid = 'public.parent'::regclass AND contype IN ('p', 'u')"

// TestSchemaApplyCockroachDBKeyMoveE2E_HappyPath moves a primary key on CockroachDB
// 26.3.5. The plan dropped the key in a statement of its own, and the server stopped
// it with `this schema change is disallowed because table "parent" is locked`. A key a
// column declares planned the drop and no addition. Each row reads the keys back, the
// table keeps its rows, and a second run finds nothing to do.
func TestSchemaApplyCockroachDBKeyMoveE2E_HappyPath(t *testing.T) {
	const rows = "INSERT INTO parent VALUES (1, 10);\n"
	tests := []struct{ name, built, desired, want string }{
		{
			name:    "a table's key",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8 NOT NULL);\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL, code INT8 NOT NULL, PRIMARY KEY (code));\n",
			want:    "parent_pkey: PRIMARY KEY (code ASC)",
		},
		{
			name:    "a column's key",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8 NOT NULL);\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL, code INT8 PRIMARY KEY);\n",
			want:    "parent_pkey: PRIMARY KEY (code ASC)",
		},
		{
			name:    "a key over more columns",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8 NOT NULL);\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL, code INT8 NOT NULL, PRIMARY KEY (id, code));\n",
			want:    "parent_pkey: PRIMARY KEY (id ASC, code ASC)",
		},
		{
			name:    "a column the plan makes NOT NULL",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8);\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL, code INT8 NOT NULL, PRIMARY KEY (code));\n",
			want:    "parent_pkey: PRIMARY KEY (code ASC)",
		},
		{
			name:    "a column the plan adds, on an empty table",
			built:   "CREATE TABLE parent (id INT8 PRIMARY KEY);\n",
			desired: "CREATE TABLE parent (id INT8 NOT NULL, code INT8 NOT NULL, PRIMARY KEY (code));\n",
			want:    "parent_pkey: PRIMARY KEY (code ASC)",
		},
		{
			name: "a key under a foreign key that a UNIQUE takes over",
			built: "CREATE TABLE parent (id INT8 PRIMARY KEY, code INT8 NOT NULL UNIQUE);\n" +
				"CREATE TABLE child (id INT8 PRIMARY KEY, pid INT8 REFERENCES parent (id));\n" + rows,
			desired: "CREATE TABLE parent (id INT8 NOT NULL UNIQUE, code INT8 NOT NULL, PRIMARY KEY (code));\n" +
				"CREATE TABLE child (id INT8 PRIMARY KEY, pid INT8 REFERENCES parent (id));\n",
			want: "parent_id_key: UNIQUE (id ASC); parent_pkey: PRIMARY KEY (code ASC)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target, _ := cockroachVisibilityDatabase(c, c.Context(), test.built)
			before := readInt(c, target, "SELECT count(*) FROM parent")

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, cockroachKeyRead), qt.Equals, test.want)
			c.Assert(readInt(c, target, "SELECT count(*) FROM parent"), qt.Equals, before)
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

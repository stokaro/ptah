//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// exclusionRetypedRead prints the exclusion constraints and the type of column r of
// public.parent as the server does.
const exclusionRetypedRead = "SELECT coalesce((SELECT string_agg(conname || ': ' || pg_get_constraintdef(oid), '; ' ORDER BY conname) " +
	"FROM pg_constraint WHERE conrelid = 'public.parent'::regclass AND contype = 'x'), '') || ' | ' || " +
	"coalesce((SELECT atttypid::regtype::text FROM pg_attribute WHERE attrelid = 'public.parent'::regclass AND attname = 'r' AND NOT attisdropped), '')"

// TestSchemaApplyExclusionOverRetypedColumnE2E_HappyPath changes the type of a column
// an exclusion constraint covers to one its index method cannot take. PostgreSQL
// rebuilds the constraint under ALTER COLUMN TYPE, so a plan that dropped it after
// that statement stopped at `data type text has no default operator class for access
// method "gist"`. Each row reads the constraints and the column back from the
// catalog, and a second run finds nothing to do.
func TestSchemaApplyExclusionOverRetypedColumnE2E_HappyPath(t *testing.T) {
	const built = "CREATE TABLE parent (id int PRIMARY KEY, r int4range, s int4range, CONSTRAINT parent_ex EXCLUDE USING gist (r WITH &&));\n" +
		"INSERT INTO parent VALUES (1, '[1,2)', '[1,2)');\n"
	tests := []struct{ name, desired, want string }{
		{
			name:    "the constraint leaves",
			desired: "CREATE TABLE parent (id int PRIMARY KEY, r text, s int4range);\n",
			want:    " | text",
		},
		{
			name:    "the constraint moves under its name",
			desired: "CREATE TABLE parent (id int PRIMARY KEY, r text, s int4range, CONSTRAINT parent_ex EXCLUDE USING gist (s WITH &&));\n",
			want:    "parent_ex: EXCLUDE USING gist (s WITH &&) | text",
		},
		{
			name:    "the constraint moves under another name",
			desired: "CREATE TABLE parent (id int PRIMARY KEY, r text, s int4range, CONSTRAINT parent_new EXCLUDE USING gist (s WITH &&));\n",
			want:    "parent_new: EXCLUDE USING gist (s WITH &&) | text",
		},
		{
			name:    "the new definition reads a column the plan adds",
			desired: "CREATE TABLE parent (id int PRIMARY KEY, r text, s int4range, u int4range, CONSTRAINT parent_ex EXCLUDE USING gist (u WITH &&));\n",
			want:    "parent_ex: EXCLUDE USING gist (u WITH &&) | text",
		},
		{
			name:    "a second column it covers leaves too",
			desired: "CREATE TABLE parent (id int PRIMARY KEY, r text);\n",
			want:    " | text",
		},
		{
			name:    "the type stays, control",
			desired: "CREATE TABLE parent (id int PRIMARY KEY, r int4range, s int4range);\n",
			want:    " | int4range",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := databaseBuiltFrom(c, built)

			run := keyedApply(c, target, test.desired)

			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", run.Stdout, run.Stderr))
			c.Assert(columnKeyString(c, target, exclusionRetypedRead), qt.Equals, test.want)
			again := keyedApply(c, target, test.desired)
			c.Assert(again.Stdout, qt.Contains, "Schema is synced")
		})
	}
}

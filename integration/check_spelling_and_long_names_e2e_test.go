//go:build integration

package integration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// printedMembership is how PostgreSQL 15.19 prints `kind IN ('a', 'b')` over a
// varchar column. Written back by a migration, it is stored as
// `= ANY (ARRAY[('a'::character varying)::text, ...])`, which is not the text
// the server printed: one pass through the server does not reach the form a
// history built from inspected databases holds.
const printedMembership = `CREATE TABLE notes (id bigint PRIMARY KEY, kind varchar,
  CONSTRAINT notes_kind_check CHECK ((kind IS NULL) OR ((kind)::text = ANY ((ARRAY['a'::character varying, 'b'::character varying])::text[]))));`

const declaredMembership = `CREATE TABLE notes (id bigint PRIMARY KEY, kind varchar,
  CONSTRAINT notes_kind_check CHECK (kind IS NULL OR kind IN ('a', 'b')));`

// longNamedKey creates a foreign key and an index whose names are past 63
// bytes, as Ent's schema export writes them. PostgreSQL creates both under
// their first 63 bytes.
var longNamedKey = `CREATE TABLE owners (id bigint PRIMARY KEY);
CREATE TABLE notes (id bigint PRIMARY KEY, owner_id bigint,
  CONSTRAINT "` + strings.Repeat("k", 70) + `" FOREIGN KEY (owner_id) REFERENCES owners (id));
CREATE INDEX "` + strings.Repeat("i", 70) + `" ON notes (owner_id);`

// TestMigrateDiffFindsTwoSpellingsOfOneDeclarationSyncedE2E replays a history
// that writes one spelling and diffs it against a schema file that writes the
// other. Each row planned a DROP and an ADD of an unchanged constraint, or
// refused the file, before the comparison learned what the rows test.
func TestMigrateDiffFindsTwoSpellingsOfOneDeclarationSyncedE2E(t *testing.T) {
	tests := []struct{ name, migration, schema string }{
		{name: "a CHECK the history wrote from its printed form", migration: printedMembership, schema: declaredMembership},
		{name: "a CHECK the schema file writes in its printed form", migration: declaredMembership, schema: printedMembership},
		{name: "a foreign key and an index named past the limit", migration: longNamedKey, schema: longNamedKey},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
		})
	}
}

// TestMigrateDiffPlansAMembershipThatChangedE2E is the control: the printed
// form of a list that gained a value is still a change.
func TestMigrateDiffPlansAMembershipThatChangedE2E(t *testing.T) {
	c := qt.New(t)
	changed := strings.Replace(declaredMembership, "('a', 'b')", "('a', 'b', 'c')", 1)
	dir, schema := writeRewrittenMigrationProject(c, printedMembership, changed)
	dev, _ := scratchReplayDatabase(c)

	out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
		"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "'c'")
}

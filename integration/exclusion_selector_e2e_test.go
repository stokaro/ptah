//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// An exclusion names a PostgreSQL EXCLUDE constraint through its table, as it
// names a check constraint, under `[type=constraint]` and under
// `[type=index]`, the type the community binary names it by. These tests hold
// an EXCLUDE constraint on each of two tables and declare neither, then apply
// with a selector that names the one on rooms through both shipped binaries.
// The apply drops the constraint on desks and leaves the one on rooms, which
// each test reads back from pg_constraint.
//
// internal/atlasfilter/exclusion_constraint_test.go holds the same selectors
// to the filter without a server.

const (
	exclusionSelectorLive = "CREATE TABLE public.rooms (id int PRIMARY KEY, slot tsrange NOT NULL, " +
		"CONSTRAINT room_slot EXCLUDE USING gist (slot WITH &&));\n" +
		"CREATE TABLE public.desks (id int PRIMARY KEY, slot tsrange NOT NULL, " +
		"CONSTRAINT desk_slot EXCLUDE USING gist (slot WITH &&));\n"
	exclusionSelectorDesired = "CREATE TABLE public.rooms (id int PRIMARY KEY, slot tsrange NOT NULL);\n" +
		"CREATE TABLE public.desks (id int PRIMARY KEY, slot tsrange NOT NULL);\n"
	// exclusionSelectorReadBack answers the EXCLUDE constraints the database
	// holds, as table.constraint.
	exclusionSelectorReadBack = "SELECT string_agg(conrelid::regclass::text || '.' || conname, ',' ORDER BY conname) " +
		"FROM pg_constraint WHERE contype = 'x'"
	exclusionSelectorAfter = "rooms.room_slot"
	dropDeskSlot           = `DROP CONSTRAINT "desk_slot"`
	roomSlot               = `"room_slot"`
)

// exclusionSelectorRows name the constraint on rooms, on a URL that names no
// schema. flags are the selector flags of the apply.
var exclusionSelectorRows = []struct {
	name  string
	flags []string
}{
	{name: "schema, table and constraint", flags: []string{"--exclude", "public.rooms.room_slot"}},
	{name: "table and constraint", flags: []string{"--exclude", "rooms.room_slot"}},
	{name: "a constraint selector", flags: []string{"--exclude", "public.rooms.room_slot[type=constraint]"}},
	{name: "an index selector", flags: []string{"--exclude", "public.rooms.room_slot[type=index]"}},
	{name: "an include of both tables", flags: []string{"--include", "public.*", "--exclude", "public.rooms.room_slot[type=constraint]"}},
}

func newExclusionSelectorFixture(c *qt.C) includeChildFixture {
	c.Helper()
	fixture := includeChildFixture{database: postgresScratchDevURL(c, "ptah_exclusion_selector")}
	execPostgresFamilySQL(c, c.Context(), fixture.database, exclusionSelectorLive)
	fixture.schema = filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(fixture.schema, []byte(exclusionSelectorDesired), 0o600), qt.IsNil)
	return fixture
}

// TestExcludeKeepsAnExclusionConstraintE2E applies through `ptah schema
// apply` and `ptah-compat schema apply` and reads the constraints back: the
// one on desks is gone and the one on rooms remains. A second apply with the
// selector finds nothing left to do.
func TestExcludeKeepsAnExclusionConstraintE2E(t *testing.T) {
	for _, row := range exclusionSelectorRows {
		t.Run(row.name+"/ptah", func(t *testing.T) {
			c := qt.New(t)
			fixture := newExclusionSelectorFixture(c)

			applied := fixture.apply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, dropDeskSlot)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), roomSlot)
			c.Assert(fixture.read(c, exclusionSelectorReadBack), qt.Equals, exclusionSelectorAfter)
			converged := fixture.compare(c, row.flags...)
			c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
			c.Assert(comparisonBody(c, converged.Stdout), qt.Equals, includeNoDiffs)
		})
		t.Run(row.name+"/ptah-compat", func(t *testing.T) {
			c := qt.New(t)
			fixture := newExclusionSelectorFixture(c)

			applied := fixture.compatApply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 0, historyComment(applied))
			c.Assert(applied.Stdout, qt.Contains, dropDeskSlot)
			c.Assert(applied.Stdout, qt.Not(qt.Contains), roomSlot)
			c.Assert(fixture.read(c, exclusionSelectorReadBack), qt.Equals, exclusionSelectorAfter)
			converged := fixture.compatDiff(c, row.flags...)
			c.Assert(converged.ExitCode, qt.Equals, 0, historyComment(converged))
			c.Assert(converged.Stdout, qt.Equals, includeSynced)
		})
	}
}

// exclusionSelectorBefore is what the read-back answers while neither
// constraint was dropped.
const exclusionSelectorBefore = "desks.desk_slot,rooms.room_slot"

// exclusionSelectorRefusedRows name the constraint on rooms under a type that
// is not one of its own, or include it on its own. Each is refused before the
// apply changes anything: a selector that names nothing protects nothing, and
// a constraint rides along with its table. The community binary accepts each
// of these excludes and drops the constraint; it has no --include.
var exclusionSelectorRefusedRows = []struct {
	name  string
	flags []string
	// wantErr is the refusal both binaries print.
	wantErr string
}{
	{
		name:    "an exclude selector",
		flags:   []string{"--exclude", "public.rooms.room_slot[type=exclude]"},
		wantErr: `the --exclude selection matched no objects: "public.rooms.room_slot[type=exclude]"`,
	},
	{
		name:    "a check selector",
		flags:   []string{"--exclude", "public.rooms.room_slot[type=check]"},
		wantErr: `the --exclude selection matched no objects: "public.rooms.room_slot[type=check]"`,
	},
	{
		name:    "a policy selector",
		flags:   []string{"--exclude", "public.rooms.room_slot[type=policy]"},
		wantErr: `the --exclude selection matched no objects: "public.rooms.room_slot[type=policy]"`,
	},
	{
		name:  "an include of the constraint",
		flags: []string{"--include", "public.rooms.room_slot[type=constraint]"},
		wantErr: `unsupported Atlas include selector "public.rooms.room_slot[type=constraint]": ` +
			"constraint resources ride along with their parent and cannot be included on their own",
	},
}

// TestExcludeRefusesAnExclusionConstraintSelectorE2E applies each refused
// selector through both shipped binaries and reads the constraints back:
// both are still there.
func TestExcludeRefusesAnExclusionConstraintSelectorE2E(t *testing.T) {
	for _, row := range exclusionSelectorRefusedRows {
		t.Run(row.name+"/ptah", func(t *testing.T) {
			c := qt.New(t)
			fixture := newExclusionSelectorFixture(c)

			applied := fixture.apply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 2, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, row.wantErr)
			c.Assert(fixture.read(c, exclusionSelectorReadBack), qt.Equals, exclusionSelectorBefore)
		})
		t.Run(row.name+"/ptah-compat", func(t *testing.T) {
			c := qt.New(t)
			fixture := newExclusionSelectorFixture(c)

			applied := fixture.compatApply(c, row.flags...)

			c.Assert(applied.ExitCode, qt.Equals, 1, historyComment(applied))
			c.Assert(applied.Stderr, qt.Contains, row.wantErr)
			c.Assert(fixture.read(c, exclusionSelectorReadBack), qt.Equals, exclusionSelectorBefore)
		})
	}
}

package atlasfilter_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/atlasfilter"
)

// An exclusion names a PostgreSQL EXCLUDE constraint through its table, as it
// names a check constraint: `rooms.room_slot` or `public.rooms.room_slot`.
// Both `[type=constraint]` and `[type=index]` reach it, since the index that
// enforces the constraint carries its name. The fixture is two tables of the
// public schema, rooms holding room_slot and desks holding desk_slot, on both
// sides of a comparison; the database side also holds each constraint's
// enforcing index, managed by the constraint.
//
// integration/exclusion_selector_e2e_test.go runs the same selectors through
// the shipped binaries against PostgreSQL.

// selectorExclusions are the constraints the fixture holds, as table and
// constraint name.
var selectorExclusions = [][2]string{{"rooms", "room_slot"}, {"desks", "desk_slot"}}

func selectorExclusionRef(table, name string) objectidentity.ID {
	return pgtable.ExclusionRef("public."+table, name, identifier.ForDialect("postgres"))
}

func observedSelectorExclusions(c *qt.C) *catalog.Database {
	c.Helper()
	database := &catalog.Database{Tables: []catalog.Table{{Schema: "public", Name: "rooms"}, {Schema: "public", Name: "desks"}}}
	objects := make([]schemaext.Object, 0, len(selectorExclusions))
	for _, exclusion := range selectorExclusions {
		ref := selectorExclusionRef(exclusion[0], exclusion[1])
		objects = append(objects, schemaext.Object{Ref: ref, Value: &pgtable.ObservedExclusion{UsingMethod: "gist", Elements: "slot WITH &&"}})
		database.Indexes = append(database.Indexes, catalog.Index{Schema: "public", TableName: exclusion[0], Name: exclusion[1], ManagedBy: ref})
	}
	database.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
	return database
}

func desiredSelectorExclusions(c *qt.C) *schemamodel.Database {
	c.Helper()
	objects := make([]schemaext.Object, 0, len(selectorExclusions))
	for _, exclusion := range selectorExclusions {
		objects = append(objects, schemaext.Object{
			Ref:   selectorExclusionRef(exclusion[0], exclusion[1]),
			Value: &pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{UsingMethod: "gist", Elements: "slot WITH &&"}},
		})
	}
	return &schemamodel.Database{
		Tables:         []schemamodel.Table{{Schema: "public", Name: "rooms", StructName: "Rooms"}, {Schema: "public", Name: "desks", StructName: "Desks"}},
		FeatureObjects: must.Must(schemaext.NewObjects(objects...)),
	}
}

func TestExcludeNamesAnExclusionConstraintThroughItsTable_HappyPath(t *testing.T) {
	tests := []struct {
		name, exclude string
		// want are the constraints both sides keep, and wantIndexes the
		// enforcing indexes the database side keeps. An index leaves with the
		// constraint that manages it, whichever type named the constraint.
		want, wantIndexes []string
	}{
		{
			name:        "schema, table and constraint",
			exclude:     "public.rooms.room_slot",
			want:        []string{"public.desks.desk_slot"},
			wantIndexes: []string{"desk_slot"},
		},
		{
			name:        "table and constraint",
			exclude:     "rooms.room_slot",
			want:        []string{"public.desks.desk_slot"},
			wantIndexes: []string{"desk_slot"},
		},
		{
			name:        "a constraint selector",
			exclude:     "public.rooms.room_slot[type=constraint]",
			want:        []string{"public.desks.desk_slot"},
			wantIndexes: []string{"desk_slot"},
		},
		{
			name:        "an index selector",
			exclude:     "public.rooms.room_slot[type=index]",
			want:        []string{"public.desks.desk_slot"},
			wantIndexes: []string{"desk_slot"},
		},
		{
			name:        "the other table",
			exclude:     "desks.desk_slot[type=constraint]",
			want:        []string{"public.rooms.room_slot"},
			wantIndexes: []string{"room_slot"},
		},
		{
			name:        "every constraint of one table",
			exclude:     "public.rooms.*[type=constraint]",
			want:        []string{"public.desks.desk_slot"},
			wantIndexes: []string{"desk_slot"},
		},
		{
			name:        "every constraint",
			exclude:     "*[type=constraint]",
			wantIndexes: make([]string, 0),
		},
		{
			name:        "every index",
			exclude:     "*[type=index]",
			wantIndexes: make([]string, 0),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			observed := observedSelectorExclusions(c)

			current, currentReport, currentErr := atlasfilter.ExcludeDatabaseScopeReport(observed, realmScope(test.exclude))
			declared, declaredReport, declaredErr := atlasfilter.ExcludeGeneratedScopeReport(desiredSelectorExclusions(c), realmScope(test.exclude))

			c.Assert(currentErr, qt.IsNil)
			c.Assert(declaredErr, qt.IsNil)
			c.Assert(policyPaths(current.FeatureObjects.Refs()), qt.DeepEquals, test.want)
			c.Assert(policyPaths(declared.FeatureObjects.Refs()), qt.DeepEquals, test.want)
			c.Assert(indexNames(current.Indexes), qt.DeepEquals, test.wantIndexes)
			c.Assert(current.Tables, qt.HasLen, 2)
			c.Assert(declared.Tables, qt.HasLen, 2)
			c.Assert(currentReport.Unmatched, qt.IsNil)
			c.Assert(declaredReport.Unmatched, qt.IsNil)
			c.Assert(observed.FeatureObjects.Refs(), qt.HasLen, len(selectorExclusions))
		})
	}
}

// TestExcludeNamesAnExclusionConstraintThroughItsTable_FailurePath names the
// constraint without its table, under a type that is not one of its own, and
// on a table that does not hold it. None names a constraint, so both sides
// keep both, and the report says the selector matched nothing.
func TestExcludeNamesAnExclusionConstraintThroughItsTable_FailurePath(t *testing.T) {
	tests := []struct {
		name, exclude string
	}{
		{name: "schema and constraint", exclude: "public.room_slot[type=constraint]"},
		{name: "a type the community binary does not name it by", exclude: "public.rooms.room_slot[type=exclude]"},
		{name: "another owner's type", exclude: "public.rooms.room_slot[type=policy]"},
		{name: "a table that does not hold it", exclude: "public.desks.room_slot[type=constraint]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			current, currentReport, currentErr := atlasfilter.ExcludeDatabaseScopeReport(observedSelectorExclusions(c), realmScope(test.exclude))
			declared, declaredReport, declaredErr := atlasfilter.ExcludeGeneratedScopeReport(desiredSelectorExclusions(c), realmScope(test.exclude))

			c.Assert(currentErr, qt.IsNil)
			c.Assert(declaredErr, qt.IsNil)
			c.Assert(policyPaths(current.FeatureObjects.Refs()), qt.DeepEquals, []string{"public.desks.desk_slot", "public.rooms.room_slot"})
			c.Assert(policyPaths(declared.FeatureObjects.Refs()), qt.DeepEquals, []string{"public.desks.desk_slot", "public.rooms.room_slot"})
			c.Assert(indexNames(current.Indexes), qt.DeepEquals, []string{"room_slot", "desk_slot"})
			c.Assert(currentReport.Unmatched, qt.DeepEquals, []string{test.exclude})
			c.Assert(declaredReport.Unmatched, qt.DeepEquals, []string{test.exclude})
		})
	}
}

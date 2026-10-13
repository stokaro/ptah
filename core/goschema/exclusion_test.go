package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/pgtablesource"
)

func TestParseSource_ExclusionComment(t *testing.T) {
	for _, test := range []struct {
		name, comment string
		want          pgtable.Exclusion
	}{
		{name: "no_overlapping_bookings",
			comment: `//ptah:schema:exclude name="no_overlapping_bookings" using="gist" elements="room_id WITH =, during WITH &&" condition="is_active = true" comment="Prevent overlapping bookings"`,
			want:    pgtable.Exclusion{UsingMethod: "gist", Elements: "room_id WITH =, during WITH &&", WhereCondition: "is_active = true", Comment: "Prevent overlapping bookings"}},
		{name: "unique_locations",
			comment: `//ptah:schema:exclude name="unique_locations" using="gist" elements="location WITH &&"`,
			want:    pgtable.Exclusion{UsingMethod: "gist", Elements: "location WITH &&"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owners, err := annotation.NewSet(pgtablesource.ExclusionAnnotations())
			c.Assert(err, qt.IsNil)
			source := "package test\n//ptah:schema:table name=\"bookings\"\n" + test.comment + "\ntype TestStruct struct{}\n"
			db, err := goschema.ParseSource(owners, "constraints.go", source)
			c.Assert(err, qt.IsNil)
			c.Assert(db.Constraints, qt.HasLen, 0)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 1)
			ref := pgtable.ExclusionRef("bookings", test.name, identifier.ForDialect("postgres"))
			object, found, err := db.FeatureObjects.Get(ref)
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(object.Value, qt.DeepEquals, &pgtable.DesiredExclusion{StructName: "TestStruct", Exclusion: test.want})
		})
	}
}

func TestParseSource_RejectsUnknownCommonConstraintKinds(t *testing.T) {
	for _, kind := range []string{"EXCLUDE", "UNQIUE", ""} {
		t.Run(kind, func(t *testing.T) {
			c := qt.New(t)
			source := "package test\n//ptah:schema:constraint name=\"c\" type=\"" + kind + "\"\ntype TestStruct struct{}\n"
			db, err := goschema.ParseSource(annotation.None(), "constraints.go", source)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*constraint "c" has unsupported common kind .*`)
			c.Assert(db.Constraints, qt.HasLen, 0)
		})
	}
}

package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// An ignored extension is one nothing is planned for, in either direction.
//
// The list used to be a filter of its own, applied before the comparison and 80
// lines from the coverage gate that answers the same question. Two answers to
// one question drift, and the prose had already drifted: the comparator's own
// doc block said an ignored extension "can still be created if defined in the
// target schema", which the filter had never done (stokaro/ptah#3373).
//
// The list is coverage records on both sides now. What it means is unchanged
// and now stated once: neither side is authoritative about the object.

// ignoredExtensionName is the extension every case below is about.
const ignoredExtensionName = "pg_trgm"

// TestIgnoredExtension_RemovalIsWithheld is what the ignore list has always
// promised: an extension the database carries and the description does not
// declare is left alone.
func TestIgnoredExtension_RemovalIsWithheld(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{}
	database := &catalog.Database{
		Extensions: []catalog.Extension{{Name: ignoredExtensionName, Schema: "public"}},
	}

	diff := schemadiff.CompareWithOptions(desired, database,
		config.WithAdditionalIgnoredExtensions(ignoredExtensionName))

	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0, qt.Commentf("removed: %#v", diff.ExtensionsRemoved))
}

// TestIgnoredExtension_RemovalIsPlannedWithoutTheList is the control for the
// case above. Without it, a comparison that planned no removal at all would
// satisfy that assertion just as well.
func TestIgnoredExtension_RemovalIsPlannedWithoutTheList(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{}
	database := &catalog.Database{
		Extensions: []catalog.Extension{{Name: ignoredExtensionName, Schema: "public"}},
	}

	diff := schemadiff.CompareWithOptions(desired, database, config.DefaultCompareOptions())

	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 1, qt.Commentf("removed: %#v", diff.ExtensionsRemoved))
}

// TestIgnoredExtension_AdditionIsWithheldToo is the other direction, and the
// one a `schema diff` from a hand-authored file to a live PostgreSQL database
// meets on every run: the file describes no plpgsql, every server has one, and
// the list is what keeps CREATE EXTENSION "plpgsql" out of the answer.
func TestIgnoredExtension_AdditionIsWithheldToo(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Extensions: []schemamodel.Extension{{Name: ignoredExtensionName, Schema: "public"}},
	}
	database := &catalog.Database{}

	diff := schemadiff.CompareWithOptions(desired, database,
		config.WithAdditionalIgnoredExtensions(ignoredExtensionName))

	c.Assert(diff.ExtensionsAdded, qt.HasLen, 0, qt.Commentf("added: %#v", diff.ExtensionsAdded))
}

// TestIgnoredExtension_AdditionIsPlannedWithoutTheList is its control. Without
// it, a comparison that planned no addition at all would satisfy the case above
// just as well.
func TestIgnoredExtension_AdditionIsPlannedWithoutTheList(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Extensions: []schemamodel.Extension{{Name: ignoredExtensionName, Schema: "public"}},
	}
	database := &catalog.Database{}

	diff := schemadiff.CompareWithOptions(desired, database, config.DefaultCompareOptions())

	c.Assert(diff.ExtensionsAdded, qt.HasLen, 1, qt.Commentf("added: %#v", diff.ExtensionsAdded))
	c.Assert(diff.ExtensionsAdded[0].Name, qt.Equals, ignoredExtensionName)
}

// TestIgnoredExtension_AgreesWithTheDirective compares the two spellings of one
// request on one pair of descriptions, which is the property this change is
// about. Asserting each against a literal would let both drift together.
func TestIgnoredExtension_AgreesWithTheDirective(t *testing.T) {
	c := qt.New(t)
	declared := func() *schemamodel.Database {
		return &schemamodel.Database{
			Extensions: []schemamodel.Extension{{Name: ignoredExtensionName, Schema: "public"}},
		}
	}
	carried := &catalog.Database{
		Extensions: []catalog.Extension{{Name: "postgis", Schema: "public"}},
	}

	byList := schemadiff.CompareWithOptions(declared(), carried,
		config.WithAdditionalIgnoredExtensions("postgis"))

	described := declared()
	described.NotDescribed = described.NotDescribed.WithObject(coverage.Extension, "postgis")
	byDirective := schemadiff.CompareWithOptions(described, carried, config.DefaultCompareOptions())

	c.Assert(byList.ExtensionsAdded, qt.DeepEquals, byDirective.ExtensionsAdded)
	c.Assert(byList.ExtensionsRemoved, qt.DeepEquals, byDirective.ExtensionsRemoved)
}

// TestIgnoredExtension_DefaultStillCoversPlpgsql keeps the default from being
// lost in the move. plpgsql is installed by PostgreSQL itself, no description
// declares it, and dropping it is the failure the default exists to prevent.
func TestIgnoredExtension_DefaultStillCoversPlpgsql(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{}
	database := &catalog.Database{
		Extensions: []catalog.Extension{{Name: "plpgsql", Schema: "pg_catalog"}},
	}

	diff := schemadiff.CompareWithOptions(desired, database, config.DefaultCompareOptions())

	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0, qt.Commentf("removed: %#v", diff.ExtensionsRemoved))
}

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

// An ignored extension and one a description declines to describe are the same
// request, and they now produce the same plan.
//
// They did not. The ignore list was a filter applied before the comparison, so
// it cut the DESIRED side as well: an extension a description declared and the
// options ignored was never created, and nothing reported the omission. The
// directive spelling of the same request created it, and CompareOptions
// documented the directive's behavior for the list (stokaro/ptah#3373).

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

// TestIgnoredExtension_DeclaredIsStillCreated is the half the two spellings
// disagreed on. An extension the description declares is created whether or not
// the list names it: the list says what not to REMOVE, and a declaration is not
// a removal.
func TestIgnoredExtension_DeclaredIsStillCreated(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Extensions: []schemamodel.Extension{{Name: ignoredExtensionName, Schema: "public"}},
	}
	database := &catalog.Database{}

	diff := schemadiff.CompareWithOptions(desired, database,
		config.WithAdditionalIgnoredExtensions(ignoredExtensionName))

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

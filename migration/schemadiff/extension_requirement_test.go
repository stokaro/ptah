package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// readingsDeclaring is a desired schema that declares the table readings with
// facets, and claims to describe every extension while it declares none.
func readingsDeclaring(facets schemaext.Facets) *schemamodel.Database {
	known := must.Must(pgextension.CompleteCoverage(schemaext.Desired))
	known = must.Must(known.Combine(must.Must(tsschema.CompleteCoverage(schemaext.Desired))))
	return &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "Reading", Name: "readings", Facets: facets}},
		Fields:          []schemamodel.Field{{StructName: "Reading", Name: "time", Type: "TIMESTAMPTZ", Nullable: false}},
		FeatureCoverage: known,
	}
}

// withTimescale is a database that holds the timescaledb extension and
// nothing else, read by a reader that describes every extension.
func withTimescale() *catalog.Database {
	return &catalog.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgextension.ObservedObject(pgextension.Ref("timescaledb"),
			pgextension.ObservedExtension{Schema: "public", Version: "2.30.2"})))),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
}

// extensionChanges returns the extension owner's changes a diff holds.
func extensionChanges(diff *difftypes.SchemaDiff) []*pgextension.Change {
	var changes []*pgextension.Change
	for _, record := range diff.FeatureChanges {
		if change, ok := record.Value.(*pgextension.Change); ok {
			changes = append(changes, change)
		}
	}
	return changes
}

// TestCompare_KeepsTheExtensionAHypertableNeeds pins, through the bundled
// runtime, that a declared hypertable counts as needing timescaledb: a
// desired schema that declares one keeps the extension although it claims to
// describe every extension and names none.
func TestCompare_KeepsTheExtensionAHypertableNeeds(t *testing.T) {
	c := qt.New(t)
	desired := readingsDeclaring(must.Must(schemaext.NewFacets(&tsschema.DesiredHypertable{Column: "time"})))

	diff, err := schemadiff.CompareWithOptions(t.Context(), desired, withTimescale(), &config.CompareOptions{Dialect: "postgres"}, builtintest.Runtime())

	c.Assert(err, qt.IsNil)
	c.Assert(extensionChanges(diff), qt.HasLen, 0)
}

// TestCompare_DropsAnExtensionNothingNeeds is the control: the same schema
// without the hypertable plans the drop, so the rule above is the
// hypertable's and not a blanket one for timescaledb.
func TestCompare_DropsAnExtensionNothingNeeds(t *testing.T) {
	c := qt.New(t)
	desired := readingsDeclaring(schemaext.Facets{})

	diff, err := schemadiff.CompareWithOptions(t.Context(), desired, withTimescale(), &config.CompareOptions{Dialect: "postgres"}, builtintest.Runtime())

	c.Assert(err, qt.IsNil)
	c.Assert(extensionChanges(diff), qt.DeepEquals, []*pgextension.Change{{Before: &pgextension.ObservedExtension{Schema: "public", Version: "2.30.2"}}})
}

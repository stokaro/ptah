package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgextension"
	"ptah.run/migration/schemadiff"
)

// An ignored extension is one nothing is planned for, in either direction.
// The run's ignore list reaches the extension owner as exclusions, and the
// owner plans nothing for an excluded extension and reports nothing undecided
// about it (stokaro/ptah#3373).

// ignoredExtensionName is the extension every case below is about.
const ignoredExtensionName = "pg_trgm"

// declaringOwnerExtensions is a desired schema that declares each name, in
// public, to the extension owner and claims that it declares every extension.
func declaringOwnerExtensions(names ...string) *schemamodel.Database {
	objects := make([]schemaext.Object, 0, len(names))
	for _, name := range names {
		objects = append(objects, must.Must(pgextension.DesiredObject(pgextension.Ref(name), pgextension.DesiredExtension{Schema: "public"})))
	}
	return &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Desired)),
	}
}

// holdingOwnerExtensions is a read that holds each name, installed in schema,
// and describes every extension.
func holdingOwnerExtensions(schema string, names ...string) *catalog.Database {
	objects := make([]schemaext.Object, 0, len(names))
	for _, name := range names {
		objects = append(objects, must.Must(pgextension.ObservedObject(pgextension.Ref(name), pgextension.ObservedExtension{Schema: schema})))
	}
	return &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(pgextension.CompleteCoverage(schemaext.Observed)),
	}
}

// installedAndDropped names the extensions a diff installs and drops, from
// the extension owner's changes.
func installedAndDropped(diff *difftypes.SchemaDiff) (installed, dropped []string) {
	for _, record := range diff.FeatureChanges {
		change, ok := record.Value.(*pgextension.Change)
		switch {
		case !ok:
		case change.Before == nil:
			installed = append(installed, pgextension.Name(record.Subject))
		case change.After == nil:
			dropped = append(dropped, pgextension.Name(record.Subject))
		}
	}
	return installed, dropped
}

// onPostgres is options as a PostgreSQL run configures them.
func onPostgres(options *config.CompareOptions) *config.CompareOptions {
	options.Dialect = "postgres"
	return options
}

// TestIgnoredExtension_RemovalIsWithheld is what the ignore list has always
// promised: an extension the database carries and the description does not
// declare is left alone.
func TestIgnoredExtension_RemovalIsWithheld(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithOptions(t.Context(), declaringOwnerExtensions(), holdingOwnerExtensions("public", ignoredExtensionName),
		onPostgres(config.WithAdditionalIgnoredExtensions(ignoredExtensionName)), must.Must(builtin.New())))

	c.Assert(extensionChanges(diff), qt.HasLen, 0)
}

// TestIgnoredExtension_RemovalIsPlannedWithoutTheList is the control for the
// case above. Without it, a comparison that planned no removal at all would
// satisfy that assertion just as well.
func TestIgnoredExtension_RemovalIsPlannedWithoutTheList(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithOptions(t.Context(), declaringOwnerExtensions(), holdingOwnerExtensions("public", ignoredExtensionName),
		onPostgres(config.DefaultCompareOptions()), must.Must(builtin.New())))

	installed, dropped := installedAndDropped(diff)
	c.Assert(installed, qt.HasLen, 0)
	c.Assert(dropped, qt.DeepEquals, []string{ignoredExtensionName})
}

// TestIgnoredExtension_AdditionIsWithheldToo is the other direction, and the
// one a `schema diff` from a hand-authored file to a live PostgreSQL database
// meets on every run: the list is what keeps CREATE EXTENSION out of the
// answer for an extension the run leaves to someone else.
func TestIgnoredExtension_AdditionIsWithheldToo(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithOptions(t.Context(), declaringOwnerExtensions(ignoredExtensionName), holdingOwnerExtensions("public"),
		onPostgres(config.WithAdditionalIgnoredExtensions(ignoredExtensionName)), must.Must(builtin.New())))

	c.Assert(extensionChanges(diff), qt.HasLen, 0)
}

// TestIgnoredExtension_AdditionIsPlannedWithoutTheList is its control. Without
// it, a comparison that planned no addition at all would satisfy the case above
// just as well.
func TestIgnoredExtension_AdditionIsPlannedWithoutTheList(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithOptions(t.Context(), declaringOwnerExtensions(ignoredExtensionName), holdingOwnerExtensions("public"),
		onPostgres(config.DefaultCompareOptions()), must.Must(builtin.New())))

	installed, dropped := installedAndDropped(diff)
	c.Assert(installed, qt.DeepEquals, []string{ignoredExtensionName})
	c.Assert(dropped, qt.HasLen, 0)
}

// TestIgnoredExtension_AgreesWithTheDirective compares the two spellings of one
// request on one pair of descriptions: the ignore list, and a not-described
// record of the extension. Asserting each against a literal would let both
// drift together.
func TestIgnoredExtension_AgreesWithTheDirective(t *testing.T) {
	c := qt.New(t)
	carried := holdingOwnerExtensions("public", "postgis")

	byList := must.Must(schemadiff.CompareWithOptions(t.Context(), declaringOwnerExtensions(ignoredExtensionName), carried,
		onPostgres(config.WithAdditionalIgnoredExtensions("postgis")), must.Must(builtin.New())))

	described := declaringOwnerExtensions(ignoredExtensionName)
	described.NotDescribed = described.NotDescribed.WithObject(coverage.Extension, "postgis")
	byDirective := must.Must(schemadiff.CompareWithOptions(t.Context(), described, carried, onPostgres(config.DefaultCompareOptions()), must.Must(builtin.New())))

	c.Assert(extensionChanges(byList), qt.DeepEquals, extensionChanges(byDirective))
	installed, dropped := installedAndDropped(byList)
	c.Assert(installed, qt.DeepEquals, []string{ignoredExtensionName})
	c.Assert(dropped, qt.HasLen, 0)
}

// TestIgnoredExtension_DefaultStillCoversPlpgsql keeps the default from being
// lost in the move. plpgsql is installed by PostgreSQL itself, no description
// declares it, and dropping it is the failure the default exists to prevent.
func TestIgnoredExtension_DefaultStillCoversPlpgsql(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithOptions(t.Context(), declaringOwnerExtensions(), holdingOwnerExtensions("pg_catalog", "plpgsql"),
		onPostgres(config.DefaultCompareOptions()), must.Must(builtin.New())))

	c.Assert(extensionChanges(diff), qt.HasLen, 0)
}

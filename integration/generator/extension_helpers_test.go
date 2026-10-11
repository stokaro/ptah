//go:build integration

package generator_test

import (
	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
)

// declaringExtensions is a desired schema that declares the extensions to
// their owner, and claims that it declares every extension, as a source does.
func declaringExtensions(c *qt.C, extensions map[string]pgextension.DesiredExtension) *schemamodel.Database {
	c.Helper()
	known, err := pgextension.CompleteCoverage(schemaext.Desired)
	c.Assert(err, qt.IsNil)
	database := &schemamodel.Database{FeatureCoverage: known}
	for name, extension := range extensions {
		object, err := pgextension.DesiredObject(pgextension.Ref(name), extension)
		c.Assert(err, qt.IsNil)
		database.FeatureObjects, err = database.FeatureObjects.With(object)
		c.Assert(err, qt.IsNil)
	}
	return database
}

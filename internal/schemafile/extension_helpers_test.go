package schemafile_test

import (
	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
)

// ownerExtensions returns the extensions a parsed document hands to the
// extension owner, keyed by name.
func ownerExtensions(c *qt.C, db *schemamodel.Database) map[string]pgextension.DesiredExtension {
	c.Helper()
	all, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	extensions := make(map[string]pgextension.DesiredExtension)
	for _, object := range all {
		if extension, ok := object.Value.(*pgextension.DesiredExtension); ok {
			extensions[pgextension.Name(object.Ref)] = *extension
		}
	}
	return extensions
}

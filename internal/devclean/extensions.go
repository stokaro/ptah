package devclean

import (
	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
)

// DeclaredExtensions is the set of extensions desired declares, in the common
// model and as the extension owner's objects: the names
// [Baseline.WithoutEnvironment] keeps. A nil desired declares none.
func DeclaredExtensions(desired *schemamodel.Database) map[string]bool {
	names := make(map[string]bool)
	if desired == nil {
		return names
	}
	for _, extension := range desired.Extensions {
		names[extension.Name] = true
	}
	for _, name := range pgextension.Names(desired.FeatureObjects) {
		names[name] = true
	}
	return names
}

// ReadExtensions is [DeclaredExtensions] for a read: the extensions it holds,
// in the common list and as the extension owner's objects. A nil read holds
// none.
func ReadExtensions(read *catalog.Database) map[string]bool {
	names := make(map[string]bool)
	if read == nil {
		return names
	}
	for _, extension := range read.Extensions {
		names[extension.Name] = true
	}
	for _, name := range pgextension.Names(read.FeatureObjects) {
		names[name] = true
	}
	return names
}

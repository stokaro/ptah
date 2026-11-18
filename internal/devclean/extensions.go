package devclean

import (
	"ptah.run/catalog"
	"ptah.run/feature/pgextension"
)

// ReadExtensions is the set of extensions read holds as the extension owner's
// objects. A nil read holds none.
func ReadExtensions(read *catalog.Database) map[string]bool {
	names := make(map[string]bool)
	if read == nil {
		return names
	}
	for _, name := range pgextension.Names(read.FeatureObjects) {
		names[name] = true
	}
	return names
}

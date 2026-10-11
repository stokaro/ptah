package pgextension

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// Names returns the names of the extensions objects holds, declared or
// observed, in identity order. Every other object is passed over.
func Names(objects schemaext.Objects) []string {
	var names []string
	for _, ref := range objects.Refs() {
		if ref.Kind == objectidentity.Kind(ExtensionKind) {
			names = append(names, Name(ref))
		}
	}
	return names
}

// Select returns objects without the extensions keep refuses, by name. Every
// other object stays, and objects is not changed.
func Select(objects schemaext.Objects, keep func(name string) bool) schemaext.Objects {
	return objects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind != objectidentity.Kind(ExtensionKind) || keep(Name(ref))
	})
}

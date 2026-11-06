package postgres

import (
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// removedTypeReader is a type the plan removes whose definition can say which
// relations' row types it names, under its common identity.
type removedTypeReader struct {
	id     objectidentity.ID
	reader schemaext.RelationReader
}

// removedTypeReaders are the types the plan removes that answer which
// relations they read: a domain, a composite type or a range type.
func removedTypeReaders(diff *difftypes.SchemaDiff) []removedTypeReader {
	var readers []removedTypeReader
	for _, change := range diff.FeatureChanges {
		if _, says := change.Value.(schemaext.StateRemoval); says && !schemaext.RemovesState(change.Value) {
			continue
		}
		object, found, err := diff.Features.CurrentObjects.Get(change.Subject)
		if err != nil || !found {
			continue
		}
		definer, defines := object.Value.(schemaext.TypeDefiner)
		reader, reads := object.Value.(schemaext.RelationReader)
		if defines && reads {
			readers = append(readers, removedTypeReader{id: definer.DefinedType(object.Ref).ID, reader: reader})
		}
	}
	return readers
}

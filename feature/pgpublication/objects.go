package pgpublication

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
)

// Names returns the names of the publications objects holds, declared or
// observed, in identity order. Every other object is passed over.
func Names(objects schemaext.Objects) []string {
	var names []string
	for _, ref := range objects.Refs() {
		if ref.Kind == objectidentity.Kind(PublicationKind) {
			names = append(names, Name(ref))
		}
	}
	return names
}

// Managed reports whether desired declares a publication. Ptah manages the
// publications of a database from the first one the desired schema declares:
// a schema that declares none leaves every publication alone, so one a
// replication consumer created for itself, as Debezium does, survives an
// apply. The comparison and a report of what it left out read this one rule,
// so the report names exactly the publications the comparison did not
// compare.
func Managed(desired schemaext.Objects) bool { return len(Names(desired)) != 0 }

// Selections is how a command's scope names the owner's publications: by
// name alone, with the selector type `publication`. A publication belongs to
// the database rather than to a schema, so a selection of schemas or tables
// names none, and a scope narrowed to them neither creates nor removes one.
func Selections() selectext.Extension {
	return selectext.Extension{Owner: Owner, Selections: []selectext.Selection{
		{Kind: PublicationKind, Type: "publication", Naming: selectext.DatabaseWide},
	}}
}

// Select returns objects without the publications keep refuses, by name.
// Every other object stays, and objects is not changed.
func Select(objects schemaext.Objects, keep func(name string) bool) schemaext.Objects {
	return objects.Select(func(ref objectidentity.ID) bool {
		return ref.Kind != objectidentity.Kind(PublicationKind) || keep(Name(ref))
	})
}

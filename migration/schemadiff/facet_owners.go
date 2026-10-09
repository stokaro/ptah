package schemadiff

import (
	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/internal/tableidentity"
)

type facetOwnerSlot struct {
	subject objectidentity.ID
	values  *schemaext.Facets
}

// Inventory, capture, and restoration use the same identity for every slot.
// Index owners are resolved through the common table/materialized-view rules;
// the identifier snapshot decides whether their namespace includes the table.
func declaredFacetSlots(db *schemamodel.Database, target string, semantics identifier.Semantics) []facetOwnerSlot {
	slots := make([]facetOwnerSlot, 0, len(db.Tables)+len(db.Indexes))
	for i := range db.Tables {
		table := &db.Tables[i]
		slots = append(slots, facetOwnerSlot{tableidentity.Subject(table.Schema, table.Name, target, semantics), &table.Facets})
	}
	owners := schemamodel.ResolveIndexOwners(db.Indexes, db.Tables, db.MaterializedViews)
	for i := range db.Indexes {
		index := &db.Indexes[i]
		subject := objectidentity.NewBuilder(semantics).Index(owners[i], index.Name)
		slots = append(slots, facetOwnerSlot{subject, &index.Facets})
	}
	return slots
}

func observedFacetSlots(db *catalog.Database, target string, semantics identifier.Semantics) []facetOwnerSlot {
	slots := make([]facetOwnerSlot, 0, len(db.Tables)+len(db.Indexes))
	for i := range db.Tables {
		table := &db.Tables[i]
		slots = append(slots, facetOwnerSlot{tableidentity.Subject(table.Schema, table.Name, target, semantics), &table.Facets})
	}
	for i := range db.Indexes {
		index := &db.Indexes[i]
		subject := objectidentity.NewBuilder(semantics).Index(index.QualifiedTableName(), index.Name)
		slots = append(slots, facetOwnerSlot{subject, &index.Facets})
	}
	return slots
}

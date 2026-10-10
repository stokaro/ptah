package schemadiff

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/tableidentity"
)

type facetOwnerSlot struct {
	subject objectidentity.ID
	values  *schemaext.Facets
}

// Inventory, capture, and restoration use the same identity for every slot.
// Index owners are resolved through the common table/materialized-view rules;
// the identifier snapshot decides whether their namespace includes the table.
func declaredFacetSlots(db *schemamodel.Database, target string, semantics identifier.Semantics) ([]facetOwnerSlot, error) {
	slots := make([]facetOwnerSlot, 0, len(db.Tables)+len(db.Indexes))
	for i := range db.Tables {
		table := &db.Tables[i]
		slots = append(slots, facetOwnerSlot{tableidentity.Subject(table.Schema, table.Name, target, semantics), &table.Facets})
	}
	owners := schemamodel.ResolveIndexOwners(db.Indexes, db.Tables, db.MaterializedViews)
	for i := range db.Indexes {
		index := &db.Indexes[i]
		owner, valid := tableref.Parse(owners[i])
		if !valid {
			return nil, fmt.Errorf("%w: index %q has no resolved owner", ptaherr.ErrInvalidSchemaDiff, index.Name)
		}
		subject := objectidentity.NewBuilder(semantics).IndexParts(owner.Schema, owner.Name, index.Name)
		slots = append(slots, facetOwnerSlot{subject, &index.Facets})
	}
	return slots, nil
}

func observedFacetSlots(db *catalog.Database, target string, semantics identifier.Semantics) []facetOwnerSlot {
	slots := make([]facetOwnerSlot, 0, len(db.Tables)+len(db.Indexes))
	for i := range db.Tables {
		table := &db.Tables[i]
		slots = append(slots, facetOwnerSlot{tableidentity.Subject(table.Schema, table.Name, target, semantics), &table.Facets})
	}
	builder := objectidentity.NewBuilder(semantics)
	for i := range db.Indexes {
		index := &db.Indexes[i]
		subject := builder.IndexParts(index.Schema, index.TableName, index.Name)
		if index.IsPrimary {
			// A primary key's index is its table's key, so it is named within
			// its table even where index names are the schema's: Spanner names
			// every one PRIMARY_KEY (stokaro/ptah#4287). A declaration names no
			// primary key index, so no declared slot is keyed this way.
			subject.Parent = builder.TableParts(index.Schema, index.TableName).Name
		}
		slots = append(slots, facetOwnerSlot{subject, &index.Facets})
	}
	return slots
}

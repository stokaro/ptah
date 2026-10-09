package goschematodb

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/internal/tableref"
)

type creationFacetSlot struct {
	subject objectidentity.ID
	values  *schemaext.Facets
}

func creationFacetSlots(db *schemamodel.Database, semantics identifier.Semantics) ([]creationFacetSlot, error) {
	builder := objectidentity.NewBuilder(semantics)
	slots := make([]creationFacetSlot, 0, len(db.Tables)+len(db.Indexes))
	for i := range db.Tables {
		table := &db.Tables[i]
		slots = append(slots, creationFacetSlot{builder.TableParts(table.Schema, table.Name), &table.Facets})
	}
	owners := schemamodel.ResolveIndexOwners(db.Indexes, db.Tables, db.MaterializedViews)
	for i := range db.Indexes {
		index := &db.Indexes[i]
		owner, valid := tableref.Parse(owners[i])
		if !valid {
			return nil, fmt.Errorf("%w: index %q has no resolved owner", schemaprojection.ErrInvalid, index.Name)
		}
		slots = append(slots, creationFacetSlot{builder.IndexParts(owner.Schema, owner.Name, index.Name), &index.Facets})
	}
	seen := make(map[objectidentity.Key]bool)
	for _, slot := range slots {
		if seen[slot.subject.Key()] {
			return nil, fmt.Errorf("%w: duplicate declared facet owner %s", schemaprojection.ErrInvalid, slot.subject)
		}
		seen[slot.subject.Key()] = true
	}
	return slots, nil
}

func applyCreationFacets(slots []creationFacetSlot, tables []schemaprojection.TableCreation, target string) error {
	owners := make(map[objectidentity.Key]*schemaext.Facets, len(slots))
	for _, slot := range slots {
		owners[slot.subject.Key()] = slot.values
	}
	for _, table := range tables {
		for _, record := range table.Facets {
			facets, found := owners[record.Subject.Key()]
			if !found {
				return fmt.Errorf("%w: computed facets have no declared owner %s", schemaprojection.ErrInvalid, record.Subject)
			}
			if err := applyCreationValues(facets, record.Values, target); err != nil {
				return err
			}
		}
	}
	return nil
}

func applyCreationValues(destination *schemaext.Facets, computed schemaext.Facets, target string) error {
	values, err := computed.Values()
	if err != nil {
		return err
	}
	for _, value := range values {
		_, present, err := destination.Get(value.Kind())
		if err != nil {
			return err
		}
		if present {
			*destination, err = destination.Replace(value)
		} else {
			*destination, err = destination.With(value)
			if err == nil {
				*destination, err = destination.WithTargetScope(value.Kind(), target)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

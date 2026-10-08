package dbmlrender

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/internal/featurereport"
)

// featureValues follows the same table selection and common ownership rules as
// the DBML document. Standalone objects and non-table facets remain in scope.
func (b *builder) featureValues() ([]schemaext.Value, error) {
	selected := *b.db
	selected.Tables = b.selected()
	selected.Fields = nil
	selected.Indexes = nil
	selected.Constraints = nil
	selected.Triggers = nil
	for _, table := range selected.Tables {
		selected.Fields = append(selected.Fields, b.fieldsOf(table)...)
		for _, index := range b.db.Indexes {
			if index.StructName == table.StructName {
				selected.Indexes = append(selected.Indexes, index)
			}
		}
		for _, constraint := range b.db.Constraints {
			if ownedBy(constraint, table) {
				selected.Constraints = append(selected.Constraints, constraint)
			}
		}
		for _, trigger := range b.db.Triggers {
			if trigger.Table == table.Name || trigger.Table == table.QualifiedName() {
				selected.Triggers = append(selected.Triggers, trigger)
			}
		}
	}
	owners := featurereport.NewTableOwners(b.db.Tables)
	keep := make(map[objectidentity.Key]bool, b.db.FeatureObjects.Len())
	for _, ref := range b.db.FeatureObjects.Refs() {
		position, err := owners.Resolve(ref)
		if err != nil {
			return nil, err
		}
		keep[ref.Key()] = position < 0 || b.covers(b.db.Tables[position].Name)
	}
	selected.FeatureObjects = b.db.FeatureObjects.Select(func(ref objectidentity.ID) bool { return keep[ref.Key()] })
	return featurereport.Values(&selected)
}

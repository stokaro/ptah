// Package capturefacets inventories declared table, index and constraint
// facet owners for preparation and CREATE projection. Both boundaries must
// recognize the same owner set without granting a provider authority over
// unrelated objects.
package capturefacets

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// Declared captures facet records by owner identity, including empty facet
// sets: the table's, each index's, each named constraint's, and the primary
// key's, whose settings are the ones [schemacapture.PrimaryKeyFacets] reads.
// The caller supplies the already resolved parent and identifier semantics.
// Duplicate owners wrap schemaext.ErrInvalidValue and return no inventory.
func Declared(table schemacapture.TableDeclaration, parent objectidentity.ID, semantics identifier.Semantics) (map[objectidentity.Key]schemaext.FacetRecord, error) {
	owned := make([]*schemamodel.Constraint, 0, len(table.Constraints))
	for i := range table.Constraints {
		owned = append(owned, &table.Constraints[i])
	}
	key := objectidentity.PrimaryKeyOf(parent)
	declared := map[objectidentity.Key]schemaext.FacetRecord{
		parent.Key(): {Subject: parent, Values: table.Table.Facets},
		key.Key():    {Subject: key, Values: *schemacapture.PrimaryKeyFacets(&table.Table, owned)},
	}
	builder := objectidentity.NewBuilder(semantics)
	for _, index := range table.Indexes {
		subject := builder.IndexParts(parent.Schema.Source, parent.Name.Source, index.Name)
		if _, duplicate := declared[subject.Key()]; duplicate {
			return nil, fmt.Errorf("%w: duplicate declared facet owner %s", schemaext.ErrInvalidValue, subject)
		}
		declared[subject.Key()] = schemaext.FacetRecord{Subject: subject, Values: index.Facets}
	}
	for _, constraint := range table.Constraints {
		if strings.EqualFold(strings.TrimSpace(constraint.Type), "PRIMARY KEY") {
			continue
		}
		subject, named := schemacapture.ConstraintSubject(parent, constraint.Type, constraint.Name, semantics)
		if !named {
			if !constraint.Facets.IsZero() {
				return nil, fmt.Errorf("%w: an unnamed constraint cannot own facets", schemaext.ErrInvalidValue)
			}
			continue
		}
		if _, duplicate := declared[subject.Key()]; duplicate {
			return nil, fmt.Errorf("%w: duplicate declared facet owner %s", schemaext.ErrInvalidValue, subject)
		}
		declared[subject.Key()] = schemaext.FacetRecord{Subject: subject, Values: constraint.Facets}
	}
	return declared, nil
}

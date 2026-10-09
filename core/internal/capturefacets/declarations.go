// Package capturefacets inventories declared table and index facet owners for
// preparation and CREATE projection. Both boundaries must recognize the same
// owner set without granting a provider authority over unrelated objects.
package capturefacets

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
)

// Declared captures facet records by owner identity, including empty facet sets.
// The caller supplies the already resolved parent and identifier semantics.
// Duplicate owners wrap schemaext.ErrInvalidValue and return no inventory.
func Declared(table schemacapture.TableDeclaration, parent objectidentity.ID, semantics identifier.Semantics) (map[objectidentity.Key]schemaext.FacetRecord, error) {
	declared := map[objectidentity.Key]schemaext.FacetRecord{
		parent.Key(): {Subject: parent, Values: table.Table.Facets},
	}
	builder := objectidentity.NewBuilder(semantics)
	for _, index := range table.Indexes {
		subject := builder.IndexParts(parent.Schema.Source, parent.Name.Source, index.Name)
		if _, duplicate := declared[subject.Key()]; duplicate {
			return nil, fmt.Errorf("%w: duplicate declared facet owner %s", schemaext.ErrInvalidValue, subject)
		}
		declared[subject.Key()] = schemaext.FacetRecord{Subject: subject, Values: index.Facets}
	}
	return declared, nil
}

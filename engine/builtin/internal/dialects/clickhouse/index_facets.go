package clickhouse

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chresolve"
	"ptah.run/dialect/clickhouse/chschema"
)

// ValidateIndexFacets checks the index values this renderer consumes. An empty
// collection, including retained source exclusions, is valid. Unknown active
// kinds wrap ErrUnsupportedFeature; observations and malformed declarations
// wrap schemaext.ErrInvalidValue. Target selection happens before this call.
func ValidateIndexFacets(facets schemaext.Facets) error {
	for _, kind := range facets.Kinds() {
		if kind != chschema.IndexKind {
			return fmt.Errorf("%w: ClickHouse index facet %q is not supported", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	value, found, err := schemaext.FacetAs[*chschema.DesiredIndex](facets, chschema.IndexKind)
	if err != nil || !found {
		return err
	}
	return chschema.ValidateDesiredIndex(value)
}

// skippingIndexSettings resolves the type and granularity a new index is
// created with. They come from the owner's facet alone: an index without one
// takes minmax and one granule, as a declaration that leaves them out does.
// The common Type field is not a second spelling of the skipping-index type.
func skippingIndexSettings(node *ast.IndexNode) (*chschema.ObservedIndex, error) {
	if err := ValidateIndexFacets(node.Facets); err != nil {
		return nil, err
	}
	value, found, err := schemaext.FacetAs[*chschema.DesiredIndex](node.Facets, chschema.IndexKind)
	if err != nil {
		return nil, err
	}
	if !found {
		value = &chschema.DesiredIndex{}
	}
	resolved, err := chresolve.Index(chresolve.IndexRequest{Desired: value, Creating: true})
	if err != nil {
		return nil, fmt.Errorf("clickhouse: index %q: %w", node.Name, err)
	}
	return resolved.Prepared.Observed()
}

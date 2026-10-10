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
	value, err := declaredIndexSettings(facets)
	if err != nil || value == nil {
		return err
	}
	return chschema.ValidateDesiredIndex(value)
}

// declaredIndexSettings returns the index's declared settings, or nil when it
// declares none. Any other active kind is refused.
func declaredIndexSettings(facets schemaext.Facets) (*chschema.DesiredIndex, error) {
	for _, kind := range facets.Kinds() {
		if kind != chschema.IndexKind {
			return nil, fmt.Errorf("%w: ClickHouse index facet %q is not supported", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	value, _, err := schemaext.FacetAs[*chschema.DesiredIndex](facets, chschema.IndexKind)
	return value, err
}

// skippingIndexSettings resolves the type and granularity a new index is
// created with. They come from the owner's facet alone: an index without one
// takes minmax and one granule, as a declaration that leaves them out does.
// The common Type field is not a second spelling of the skipping-index type.
func skippingIndexSettings(node *ast.IndexNode) (*chschema.ObservedIndex, error) {
	value, err := declaredIndexSettings(node.Facets)
	if err != nil {
		return nil, err
	}
	if value == nil {
		value = &chschema.DesiredIndex{}
	}
	// Resolution validates the declaration before it resolves the defaults.
	resolved, err := chresolve.Index(chresolve.IndexRequest{Desired: value, Creating: true})
	if err != nil {
		return nil, fmt.Errorf("clickhouse: index %q: %w", node.Name, err)
	}
	return resolved.Prepared.Observed()
}

package clickhouse

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// ValidateMaterializedViewFacets checks the materialized view values this
// renderer consumes: a refresh schedule. An empty collection is valid. Any
// other active kind wraps ErrUnsupportedFeature; an observation or a
// malformed schedule wraps schemaext.ErrInvalidValue. Target selection happens
// before this call.
func ValidateMaterializedViewFacets(facets schemaext.Facets) error {
	_, err := declaredRefresh(facets)
	return err
}

// declaredRefresh returns the view's declared schedule, or nil when it
// declares none.
func declaredRefresh(facets schemaext.Facets) (*chschema.DesiredRefresh, error) {
	for _, kind := range facets.Kinds() {
		if kind != chschema.RefreshKind {
			return nil, fmt.Errorf("%w: ClickHouse materialized view facet %q is not supported", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	value, _, err := schemaext.FacetAs[*chschema.DesiredRefresh](facets, chschema.RefreshKind)
	if err != nil || value == nil {
		return nil, err
	}
	if err := chschema.ValidateDesiredRefresh(value); err != nil {
		return nil, err
	}
	return value, nil
}

package mysqlrender

import (
	"fmt"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// ValidateIndexFacets checks the index values the MySQL-family renderer
// consumes: an index's declared options and its block-size hint. An empty
// collection is valid. Any other active kind wraps
// ptaherr.ErrUnsupportedFeature, and an invalid declaration wraps
// schemaext.ErrInvalidValue. The MariaDB limit on the hint is checked where
// the target is known.
func ValidateIndexFacets(facets schemaext.Facets) error {
	if _, err := IndexOptions(facets); err != nil {
		return err
	}
	_, _, err := mysqlschema.IndexBlockSize(facets)
	return err
}

// IndexOptions returns the options an index declares, or nil for an index
// that declares none. A kind other than the options and the block-size hint
// is refused.
func IndexOptions(facets schemaext.Facets) (*mysqlschema.DesiredIndex, error) {
	for _, kind := range facets.Kinds() {
		if kind != mysqlschema.IndexKind && kind != mysqlschema.IndexBlockSizeKind {
			return nil, fmt.Errorf("%w: MySQL index facet %q is not supported", ptaherr.ErrUnsupportedFeature, kind)
		}
	}
	value, _, err := schemaext.FacetAs[*mysqlschema.DesiredIndex](facets, mysqlschema.IndexKind)
	if err != nil || value == nil {
		return nil, err
	}
	if err := mysqlschema.ValidateDesiredIndex(value); err != nil {
		return nil, err
	}
	return value, nil
}

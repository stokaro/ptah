package mysqlcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// ColumnService is the comparison owner of [mysqlschema.ColumnSettingsKind]
// on columns.
//
// It decides that no column settings change is planned: the declared
// settings stay the effective declaration as written, nothing observed is
// adopted into it, and no change or undecided diagnostic is reported. A
// column that changes for another reason is rewritten with MODIFY COLUMN, and
// that statement writes the declared settings; a column whose only difference
// is its character set or its ON UPDATE clause keeps what it holds. Adopting
// the observed character set would instead write the table's inherited one
// into every such statement.
type ColumnService struct{}

// CompareFacets returns the declared settings unchanged, with no changes. It
// refuses another target, another kind and an owner that is not a column.
func (ColumnService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if !slices.Contains(mysqlschema.Targets(), request.Target) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: MySQL column comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{mysqlschema.ColumnSettingsKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported MySQL column comparison kinds", schemaext.ErrInvalidValue)
	}
	for _, owner := range request.Owners {
		if owner.Subject.Kind != objectidentity.KindColumn {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: MySQL column settings attached to %s", schemaext.ErrInvalidValue, owner.Subject)
		}
	}
	desired := schemaext.FacetState{Records: slices.Clone(request.Desired.Records), Coverage: request.Desired.Coverage}
	return schemaext.FacetComparisonResult{Complete: true, Desired: desired}, ctx.Err()
}

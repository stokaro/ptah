package mysqlcompare

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// TableService compares the options of MySQL and MariaDB tables. Its zero
// value is ready for concurrent use.
type TableService struct{}

// CompareFacets returns a complete comparison with no change and the
// declarations as they were. The options apply when a table is created: the
// engine and the next auto-increment value are not read, and a table that
// exists keeps the character set it holds, so no plan changes them, as no
// plan ever has. Each value is validated. Invalid input and cancellation
// return a zero result.
func (TableService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	return creationOptions[*mysqlschema.DesiredTable, *mysqlschema.ObservedTable]{
		name: "MySQL table options", kind: mysqlschema.TableKind, owner: objectidentity.KindTable,
	}.compare(ctx, request)
}

// IndexService compares the options of MySQL and MariaDB indexes. Its zero
// value is ready for concurrent use.
type IndexService struct{}

// CompareFacets returns a complete comparison with no change and the
// declarations as they were. The options apply when an index is created and
// the parser is not read, so no plan changes them, as no plan ever has. Each
// value is validated. Invalid input and cancellation return a zero result.
func (IndexService) CompareFacets(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	return creationOptions[*mysqlschema.DesiredIndex, *mysqlschema.ObservedIndex]{
		name: "MySQL index options", kind: mysqlschema.IndexKind, owner: objectidentity.KindIndex,
	}.compare(ctx, request)
}

// creationOptions compares options a statement writes only when it creates
// their owner, D declared and O observed: it validates each value and plans
// no change.
type creationOptions[D, O schemaext.Value] struct {
	name  string
	kind  schemaext.Kind
	owner objectidentity.Kind
}

func (c creationOptions[D, O]) compare(ctx context.Context, request schemaext.FacetComparisonRequest) (schemaext.FacetComparisonResult, error) {
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != platform.MySQL && request.Target != platform.MariaDB {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: %s comparison on %q", ptaherr.ErrUnsupportedDialect, c.name, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{c.kind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported %s comparison kinds", schemaext.ErrInvalidValue, c.name)
	}
	for _, owner := range request.Owners {
		if owner.Subject.Kind != c.owner {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: %s attach to a %s, not %s", schemaext.ErrInvalidValue, c.name, c.owner, owner.Subject)
		}
	}
	for _, record := range request.Desired.Records {
		if _, _, err := schemaext.FacetAs[D](record.Values, c.kind); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	for _, record := range request.Current.Records {
		if _, _, err := schemaext.FacetAs[O](record.Values, c.kind); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	return result, ctx.Err()
}

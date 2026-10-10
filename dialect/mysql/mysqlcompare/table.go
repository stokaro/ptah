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
	if ctx == nil {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.FacetComparisonResult{}, err
	}
	if request.Target != platform.MySQL && request.Target != platform.MariaDB {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: MySQL table options comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{mysqlschema.TableKind}) {
		return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: unsupported MySQL table options comparison kinds", schemaext.ErrInvalidValue)
	}
	for _, owner := range request.Owners {
		if owner.Subject.Kind != objectidentity.KindTable {
			return schemaext.FacetComparisonResult{}, fmt.Errorf("%w: MySQL table options attach to a table, not %s", schemaext.ErrInvalidValue, owner.Subject)
		}
	}
	for _, record := range request.Desired.Records {
		if _, _, err := schemaext.FacetAs[*mysqlschema.DesiredTable](record.Values, mysqlschema.TableKind); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	for _, record := range request.Current.Records {
		if _, _, err := schemaext.FacetAs[*mysqlschema.ObservedTable](record.Values, mysqlschema.TableKind); err != nil {
			return schemaext.FacetComparisonResult{}, err
		}
	}
	result := schemaext.FacetComparisonResult{Complete: true, Desired: request.Desired}
	result.Desired.Records = slices.Clone(result.Desired.Records)
	return result, ctx.Err()
}

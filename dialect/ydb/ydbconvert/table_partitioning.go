package ydbconvert

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbpartition"
)

// TablePartitioningService converts a row table's settings between their
// representations. An observation becomes a declaration naming the same
// settings; a declaration becomes what a table created from it holds where no
// table profile changes it: the declaration read over YDB's documented
// defaults, written as the settings that differ from them, with no starting
// layout, which YDB keeps no record of.
type TablePartitioningService struct{}

var tablePartitioningConversion = facetConversion[*ydbschema.DesiredTablePartitioning, *ydbschema.ObservedTablePartitioning]{
	name:     "YDB table partitioning",
	observe:  observeTablePartitioning,
	validate: ydbschema.ValidateObservedTablePartitioning,
	declare:  (*ydbschema.ObservedTablePartitioning).Desired,
}

// ConvertFeatures converts an ordered batch without mutating inputs. It accepts
// only the ydb target and opposite desired/observed representations. Invalid
// values, a declaration YDB would refuse, and invalid directions wrap
// schemaext.ErrInvalidValue; other targets wrap ptaherr.ErrUnsupportedDialect.
// Nil context is invalid. Any error, including cancellation, returns no
// partial result. An empty batch succeeds.
func (TablePartitioningService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return tablePartitioningConversion.convert(ctx, request)
}

func observeTablePartitioning(desired *ydbschema.DesiredTablePartitioning) (*ydbschema.ObservedTablePartitioning, error) {
	if err := ydbschema.ValidateDesiredTablePartitioning(desired); err != nil {
		return nil, err
	}
	settings, err := ydbpartition.HeldTable(&desired.TablePartitioning)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", schemaext.ErrInvalidValue, err)
	}
	observed := &ydbschema.ObservedTablePartitioning{}
	if spec := ydbpartition.TableSpec(settings); spec != nil {
		observed.TablePartitioning = *spec
	}
	return observed, nil
}

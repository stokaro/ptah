package ydbconvert

import (
	"context"

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
	name: "YDB table partitioning",
	observe: observeSettings(ydbschema.ValidateDesiredTablePartitioning,
		func(desired *ydbschema.DesiredTablePartitioning) *ydbschema.TablePartitioning {
			return &desired.TablePartitioning
		},
		ydbpartition.HeldTable, ydbpartition.TableSpec,
		func(settings ydbschema.TablePartitioning) *ydbschema.ObservedTablePartitioning {
			return &ydbschema.ObservedTablePartitioning{TablePartitioning: settings}
		}),
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

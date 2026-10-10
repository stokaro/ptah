package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
)

// IndexPartitioningService converts a global index's settings between their
// representations, as [TablePartitioningService] converts a table's.
type IndexPartitioningService struct{}

var indexPartitioningConversion = facetConversion[*ydbschema.DesiredIndexPartitioning, *ydbschema.ObservedIndexPartitioning]{
	name: "YDB index partitioning",
	observe: observeSettings(ydbschema.ValidateDesiredIndexPartitioning,
		func(desired *ydbschema.DesiredIndexPartitioning) *ydbschema.IndexPartitioning {
			return &desired.IndexPartitioning
		},
		ydbindex.Held, ydbindex.Spec,
		func(settings ydbschema.IndexPartitioning) *ydbschema.ObservedIndexPartitioning {
			return &ydbschema.ObservedIndexPartitioning{IndexPartitioning: settings}
		}),
	validate: ydbschema.ValidateObservedIndexPartitioning,
	declare:  (*ydbschema.ObservedIndexPartitioning).Desired,
}

// ConvertFeatures converts an ordered batch without mutating inputs; see
// [TablePartitioningService.ConvertFeatures].
func (IndexPartitioningService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return indexPartitioningConversion.convert(ctx, request)
}

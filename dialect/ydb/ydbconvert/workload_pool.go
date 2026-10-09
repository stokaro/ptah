package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
)

// PoolService converts complete pool settings without changing source knowledge.
type PoolService struct{}

// ConvertFeatures preserves raw settings and omits source provenance from observations.
func (PoolService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertStandalone(ctx, request, "pool", ydbworkload.PoolCodecs(), (*ydbworkload.DesiredPool).Observed, (*ydbworkload.ObservedPool).Desired)
}

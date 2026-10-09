package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// CoordinationService converts standalone node configuration without applying
// defaults or claiming that a projected declaration has been inspected.
type CoordinationService struct{}

// ConvertFeatures preserves raw settings and returns an independent batch.
func (CoordinationService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertStandalone(ctx, request, "coordination", ydbcoordination.Codecs(), (*ydbcoordination.Desired).Observed, (*ydbcoordination.Observed).Desired)
}

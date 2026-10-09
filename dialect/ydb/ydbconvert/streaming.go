package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbstreaming"
)

// StreamingService converts standalone query configuration without applying
// defaults or claiming that a projected declaration has been inspected.
type StreamingService struct{}

// ConvertFeatures preserves raw settings and returns an independent batch.
func (StreamingService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertStandalone(ctx, request, "streaming", ydbstreaming.Codecs(), (*ydbstreaming.Desired).Observed, (*ydbstreaming.Observed).Desired)
}

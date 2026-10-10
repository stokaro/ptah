package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TopicService converts topics between a declaration and an observation
// without resolving defaults or claiming that a projected declaration has
// been inspected.
type TopicService struct{}

// ConvertFeatures keeps every setting and consumer and returns an independent
// batch.
func (TopicService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertStandalone(ctx, request, "topic", ydbtopic.Codecs(), (*ydbtopic.Desired).Observed, (*ydbtopic.Observed).Desired)
}

package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbworkload"
)

// ClassifierService converts complete classifier settings without changing source knowledge.
type ClassifierService struct{}

// ConvertFeatures preserves raw settings and omits source provenance from observations.
func (ClassifierService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertStandalone(ctx, request, "classifier", ydbworkload.ClassifierCodecs(), (*ydbworkload.DesiredClassifier).Observed, (*ydbworkload.ObservedClassifier).Desired)
}

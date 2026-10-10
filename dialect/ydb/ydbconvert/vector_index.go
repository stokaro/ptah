package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbindex"
)

// VectorIndexService converts a vector index's settings between their
// representations. An observation becomes a declaration that keeps every
// setting; a declaration becomes the settings an index built from it holds,
// and one YDB would refuse is refused.
type VectorIndexService struct{}

var vectorConversion = facetConversion[*ydbschema.DesiredVectorIndex, *ydbschema.ObservedVectorIndex]{
	name:     "YDB vector index",
	observe:  observeVector,
	validate: ydbschema.ValidateObservedVectorIndex,
	declare:  (*ydbschema.ObservedVectorIndex).Desired,
}

func observeVector(declared *ydbschema.DesiredVectorIndex) (*ydbschema.ObservedVectorIndex, error) {
	if err := ydbschema.ValidateDesiredVectorIndex(declared); err != nil {
		return nil, err
	}
	settings, err := ydbindex.ResolveVector(new(declared.Settings()), "")
	if err != nil {
		return nil, &schemaext.InvalidModelError{Kind: ydbschema.VectorIndexKind, Representation: schemaext.Desired, Message: err.Error()}
	}
	return new(ydbschema.ObservedVectorIndex(settings)), nil
}

// ConvertFeatures converts an ordered batch without mutating inputs. It accepts
// only the ydb target and opposite desired/observed representations. Invalid
// values or directions wrap schemaext.ErrInvalidValue; other targets wrap
// ptaherr.ErrUnsupportedDialect. Nil context is invalid. Any error, including
// cancellation, returns no partial result. An empty batch succeeds.
func (VectorIndexService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return vectorConversion.convert(ctx, request)
}

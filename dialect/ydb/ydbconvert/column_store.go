package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// ColumnStoreService converts a table's column storage between its
// representations. An observation becomes a declaration that keeps it
// exactly; a declaration becomes the storage a table created from it holds,
// with the layout it states.
type ColumnStoreService struct{}

var storeConversion = facetConversion[*ydbschema.DesiredColumnStore, *ydbschema.ObservedColumnStore]{
	name:     "YDB column storage",
	observe:  (*ydbschema.DesiredColumnStore).Observed,
	validate: ydbschema.ValidateObservedColumnStore,
	declare:  (*ydbschema.ObservedColumnStore).Desired,
}

// ConvertFeatures converts an ordered batch without mutating inputs. It accepts
// only the ydb target and opposite desired/observed representations. Invalid
// values or directions wrap schemaext.ErrInvalidValue; other targets wrap
// ptaherr.ErrUnsupportedDialect. Nil context is invalid. Any error, including
// cancellation, returns no partial result. An empty batch succeeds.
func (ColumnStoreService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return storeConversion.convert(ctx, request)
}

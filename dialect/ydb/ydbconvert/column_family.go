package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// ColumnFamiliesService converts a row table's column families between their
// representations. An observation becomes a declaration that keeps every
// family exactly; a declaration becomes the families a table created from it
// holds where no table profile adds any.
type ColumnFamiliesService struct{}

var familyConversion = facetConversion[*ydbschema.DesiredColumnFamilies, *ydbschema.ObservedColumnFamilies]{
	name:     "YDB column families",
	observe:  (*ydbschema.DesiredColumnFamilies).Observed,
	validate: ydbschema.ValidateObservedColumnFamilies,
	declare:  (*ydbschema.ObservedColumnFamilies).Desired,
}

// ConvertFeatures converts an ordered batch without mutating inputs. It accepts
// only the ydb target and opposite desired/observed representations. Invalid
// values or directions wrap schemaext.ErrInvalidValue; other targets wrap
// ptaherr.ErrUnsupportedDialect. Nil context is invalid. Any error, including
// cancellation, returns no partial result. An empty batch succeeds.
func (ColumnFamiliesService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return familyConversion.convert(ctx, request)
}

package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// TTLService converts a TTL between its representations. An observation
// becomes an exact declaration without its run interval; a declaration becomes
// the TTL a CREATE or SET would leave, its interval written as YDB shows the
// seconds it keeps.
type TTLService struct{}

var ttlConversion = facetConversion[*ydbschema.DesiredTTL, *ydbschema.ObservedTTL]{
	name:     "YDB TTL",
	observe:  (*ydbschema.DesiredTTL).Observed,
	validate: ydbschema.ValidateObservedTTL,
	declare:  (*ydbschema.ObservedTTL).Desired,
}

// ConvertFeatures converts an ordered batch without mutating inputs. It accepts
// only the ydb target and opposite desired/observed representations. Invalid
// values or directions wrap schemaext.ErrInvalidValue; other targets
// wrap ptaherr.ErrUnsupportedDialect. Nil context is invalid. Any error,
// including cancellation, returns no partial result. An empty batch succeeds.
func (TTLService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return ttlConversion.convert(ctx, request)
}

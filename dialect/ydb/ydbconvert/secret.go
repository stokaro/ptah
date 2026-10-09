package ydbconvert

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbsecret"
)

// SecretService converts secrets between declarations and observations. An
// observation becomes a declaration that keeps the secret and names no
// variable, which selects the default one for its path; a declaration becomes
// the empty observation a read would list. Neither direction carries a value
// or asks for a rotation.
type SecretService struct{}

// ConvertFeatures returns an independent batch in input order.
func (SecretService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	return convertStandalone(ctx, request, "secret", ydbsecret.Codecs(), (*ydbsecret.Desired).Observed, (*ydbsecret.Observed).Desired)
}

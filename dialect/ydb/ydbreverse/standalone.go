package ydbreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

func reverseStandalone(ctx context.Context, request schemaext.ReversalRequest, family string, key capability.Capability, reverse func(schemaext.ChangeRecord) (schemaext.Reversal, error)) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Target != "ydb" {
		return nil, fmt.Errorf("%w: YDB reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !request.Identifiers.Equal(identifier.ForDialect("ydb")) {
		return nil, fmt.Errorf("%w: invalid %s reversal identifier semantics", schemaext.ErrInvalidValue, family)
	}
	if len(request.Changes) > 0 && !request.Capabilities.Has(key) {
		return nil, fmt.Errorf("%w: reversing %s requires %s", ptaherr.ErrUnsupportedFeature, family, key)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reversed, err := reverse(record)
		if err != nil {
			return nil, err
		}
		result = append(result, reversed)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

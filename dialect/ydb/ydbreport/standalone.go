package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
)

func reportStandalone(ctx context.Context, request schemaext.ReportingRequest, family string, kind schemaext.Kind, metric string, codecs []schemaext.Codec) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	var codec schemaext.Codec
	for _, candidate := range codecs {
		if candidate.Representation == request.Representation {
			codec = candidate
		}
	}
	if codec.Prototype == nil {
		return nil, fmt.Errorf("%w: %s reporting requires desired or observed state", schemaext.ErrInvalidValue, family)
	}
	result := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if _, err := codec.Clone(value); err != nil {
			return nil, err
		}
		result = append(result, schemaext.ValueReport{Kind: kind, Counts: []schemaext.MetricCount{{Name: metric, Value: 1}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

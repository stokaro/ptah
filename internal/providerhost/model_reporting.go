package providerhost

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
)

// modelReporter publishes only the owner's frozen omission label. It validates
// captured values without interpreting payloads or declaring count metrics.
type modelReporter struct{ contract *modelContract }

func (r modelReporter) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if request.Representation != r.contract.key.representation {
		return nil, fmt.Errorf("%w: reporting representation differs from model", schemaext.ErrInvalidValue)
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if _, err := r.contract.value(value); err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: r.contract.key.kind})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

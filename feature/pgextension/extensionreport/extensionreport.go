// Package extensionreport counts PostgreSQL extensions for inventory reports,
// for the owner of package pgextension. It reads captured values only.
package extensionreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// extensionsMetric is the metric the count is reported under, the name the
// common schema statistics report extensions with.
const extensionsMetric = "extensions"

// Service counts extensions. Its zero value is ready for concurrent use.
type Service struct{}

// Definitions returns the report definitions of the extension model.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: pgextension.ExtensionKind, DisplayName: "extensions", Metrics: []schemaext.MetricDefinition{
		{Name: extensionsMetric, Help: "Extensions"},
	}}}
}

// ReportValues counts each extension once, in the representation the request
// names. A value of another representation or model is refused.
func (Service) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validate(value, request.Representation); err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: value.Kind(), Counts: []schemaext.MetricCount{{Name: extensionsMetric, Value: 1}}})
	}
	return reports, ctx.Err()
}

func validate(value schemaext.Value, representation schemaext.Representation) error {
	switch typed := value.(type) {
	case *pgextension.DesiredExtension:
		if representation == schemaext.Desired {
			return pgextension.ValidateDesired(typed)
		}
	case *pgextension.ObservedExtension:
		if representation == schemaext.Observed {
			return pgextension.ValidateObserved(typed)
		}
	}
	return fmt.Errorf("%w: unexpected %s extension value %T", schemaext.ErrInvalidValue, representation, value)
}

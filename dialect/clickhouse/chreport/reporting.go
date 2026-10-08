// Package chreport describes captured ClickHouse storage settings for reports.
package chreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// Service reports captured table settings without inspecting a server. Its zero
// value is usable and safe for concurrent calls.
type Service struct{}

// Definitions returns independent omission labels and count metadata. The count
// describes captured values, never the completeness of a catalog read.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: chschema.TableKind, DisplayName: "ClickHouse table settings", Metrics: []schemaext.MetricDefinition{
		{Name: "clickhouse_table_settings", Help: "Tables with captured ClickHouse storage settings"},
	}}}
}

// ReportValues validates the representation and returns one ordered report per
// table value. Errors and cancellation expose no partial report. Nil context or
// invalid values wrap schemaext.ErrInvalidValue; inputs remain unchanged.
func (Service) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Representation != schemaext.Desired && request.Representation != schemaext.Observed {
		return nil, fmt.Errorf("%w: reporting requires a schema representation", schemaext.ErrInvalidValue)
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := validateValue(value, request.Representation); err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: chschema.TableKind, Counts: []schemaext.MetricCount{{Name: "clickhouse_table_settings", Value: 1}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

func validateValue(value schemaext.Value, representation schemaext.Representation) error {
	switch value := value.(type) {
	case *chschema.DesiredTable:
		if representation == schemaext.Desired {
			return chschema.ValidateDesired(value)
		}
	case *chschema.ObservedTable:
		if representation == schemaext.Observed {
			return chschema.ValidateObserved(value)
		}
	}
	return fmt.Errorf("%w: ClickHouse table report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}

package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// TTLService reports captured TTL without inspecting a server. Its zero
// value is usable and safe for concurrent calls.
type TTLService struct{}

// TTLDefinitions returns independent omission labels and count metadata. The count
// describes captured values, never the completeness of a catalog read.
func TTLDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbschema.TTLKind, DisplayName: "TTL settings", Metrics: []schemaext.MetricDefinition{
		{Name: "ydb_ttl_tables", Help: "Tables with a captured YDB TTL"},
	}}}
}

// ReportValues validates the representation and returns one ordered report per
// table value. Errors and cancellation expose no partial report. Nil context or
// invalid values wrap schemaext.ErrInvalidValue; inputs remain unchanged.
func (TTLService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
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
		if err := validateTTLValue(value, request.Representation); err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: ydbschema.TTLKind, Counts: []schemaext.MetricCount{{Name: "ydb_ttl_tables", Value: 1}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

func validateTTLValue(value schemaext.Value, representation schemaext.Representation) error {
	switch value := value.(type) {
	case *ydbschema.DesiredTTL:
		if representation == schemaext.Desired {
			return ydbschema.ValidateDesiredTTL(value)
		}
	case *ydbschema.ObservedTTL:
		if representation == schemaext.Observed {
			return ydbschema.ValidateObservedTTL(value)
		}
	}
	return fmt.Errorf("%w: YDB TTL report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}

// Package spannerreport describes captured Spanner row deletion policies for reports.
package spannerreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/spanner/spannerschema"
)

// Service reports captured row deletion policies without inspecting a server. Its zero
// value is usable and safe for concurrent calls.
type Service struct{}

// Definitions returns independent omission labels and count metadata. The count
// describes captured values, never the completeness of a catalog read.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: spannerschema.RowDeletionKind, DisplayName: "row deletion policies", Metrics: []schemaext.MetricDefinition{
		{Name: "spanner_row_deletion_policy_tables", Help: "Tables with a captured Spanner row deletion policy"},
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
		reports = append(reports, schemaext.ValueReport{Kind: spannerschema.RowDeletionKind, Counts: []schemaext.MetricCount{{Name: "spanner_row_deletion_policy_tables", Value: 1}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

func validateValue(value schemaext.Value, representation schemaext.Representation) error {
	switch value := value.(type) {
	case *spannerschema.DesiredRowDeletion:
		if representation == schemaext.Desired {
			return spannerschema.ValidateDesired(value)
		}
	case *spannerschema.ObservedRowDeletion:
		if representation == schemaext.Observed {
			return spannerschema.ValidateObserved(value)
		}
	}
	return fmt.Errorf("%w: Spanner row deletion policy report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}

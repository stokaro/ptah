package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbfamily"
)

// ColumnFamiliesService reports captured column families without inspecting a
// server. Its zero value is usable and safe for concurrent calls.
type ColumnFamiliesService struct{}

// ColumnFamiliesDefinitions returns independent omission labels and count
// metadata. The count describes captured values, never the completeness of a
// catalog read.
func ColumnFamiliesDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbschema.ColumnFamiliesKind, DisplayName: "column families", Metrics: []schemaext.MetricDefinition{
		{Name: "ydb_column_families", Help: "YDB column families a table states, without a default family that states nothing"},
	}}}
}

// ReportValues validates the representation and returns one ordered report per
// table value, counting the families [ydbfamily.Stated] keeps: a table nobody
// gave families counts none. Errors and cancellation expose no partial report.
// Nil context or invalid values wrap schemaext.ErrInvalidValue; inputs remain
// unchanged.
func (ColumnFamiliesService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
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
		families, err := reportedFamilies(value, request.Representation)
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: ydbschema.ColumnFamiliesKind,
			Counts: []schemaext.MetricCount{{Name: "ydb_column_families", Value: len(ydbfamily.Stated(families))}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

func reportedFamilies(value schemaext.Value, representation schemaext.Representation) ([]ydbschema.ColumnFamily, error) {
	switch value := value.(type) {
	case *ydbschema.DesiredColumnFamilies:
		if representation == schemaext.Desired {
			return value.Families, ydbschema.ValidateDesiredColumnFamilies(value)
		}
	case *ydbschema.ObservedColumnFamilies:
		if representation == schemaext.Observed {
			return value.Families, ydbschema.ValidateObservedColumnFamilies(value)
		}
	}
	return nil, fmt.Errorf("%w: YDB column family report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}

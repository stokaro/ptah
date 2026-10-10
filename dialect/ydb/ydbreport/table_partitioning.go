package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// TablePartitioningService reports captured row table settings without
// inspecting a server. Its zero value is usable and safe for concurrent calls.
type TablePartitioningService struct{}

// TablePartitioningDefinitions returns independent omission labels and count
// metadata. The count describes captured values, never the completeness of a
// catalog read.
func TablePartitioningDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbschema.TablePartitioningKind, DisplayName: "table partitioning, read replicas and key bloom filters",
		Metrics: []schemaext.MetricDefinition{
			{Name: "ydb_table_partitioning", Help: "YDB row tables stating partitioning, read replicas or a key bloom filter"},
		}}}
}

// ReportValues validates the representation and returns one ordered report per
// table value, counting it once where it states a setting. Errors and
// cancellation expose no partial report. Nil context or invalid values wrap
// schemaext.ErrInvalidValue; inputs remain unchanged.
func (TablePartitioningService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
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
		settings, err := reportedPartitioning(value, request.Representation)
		if err != nil {
			return nil, err
		}
		count := 0
		if !settings.IsZero() {
			count = 1
		}
		reports = append(reports, schemaext.ValueReport{Kind: ydbschema.TablePartitioningKind,
			Counts: []schemaext.MetricCount{{Name: "ydb_table_partitioning", Value: count}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

func reportedPartitioning(value schemaext.Value, representation schemaext.Representation) (ydbschema.TablePartitioning, error) {
	switch value := value.(type) {
	case *ydbschema.DesiredTablePartitioning:
		if representation == schemaext.Desired && value != nil {
			return value.TablePartitioning, ydbschema.ValidateDesiredTablePartitioning(value)
		}
	case *ydbschema.ObservedTablePartitioning:
		if representation == schemaext.Observed && value != nil {
			return value.TablePartitioning, ydbschema.ValidateObservedTablePartitioning(value)
		}
	}
	return ydbschema.TablePartitioning{}, fmt.Errorf("%w: YDB table partitioning report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}

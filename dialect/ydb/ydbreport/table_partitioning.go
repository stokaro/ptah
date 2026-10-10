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
	return reportSettings(ctx, request, ydbschema.TablePartitioningKind, "ydb_table_partitioning", func(value schemaext.Value) (bool, error) {
		switch value := value.(type) {
		case *ydbschema.DesiredTablePartitioning:
			if request.Representation == schemaext.Desired && value != nil {
				return !value.IsZero(), ydbschema.ValidateDesiredTablePartitioning(value)
			}
		case *ydbschema.ObservedTablePartitioning:
			if request.Representation == schemaext.Observed && value != nil {
				return !value.IsZero(), ydbschema.ValidateObservedTablePartitioning(value)
			}
		}
		return false, fmt.Errorf("%w: YDB table partitioning report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, request.Representation)
	})
}

package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// IndexPartitioningService reports captured index settings without
// inspecting a server. Its zero value is usable and safe for concurrent calls.
type IndexPartitioningService struct{}

// IndexPartitioningDefinitions returns independent omission labels and count
// metadata.
func IndexPartitioningDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbschema.IndexPartitioningKind, DisplayName: "index partitioning and read replicas",
		Metrics: []schemaext.MetricDefinition{
			{Name: "ydb_index_partitioning", Help: "YDB global indexes stating partitioning or read replicas"},
		}}}
}

// ReportValues returns one ordered report per index value, counting it once
// where it states a setting; see [TablePartitioningService.ReportValues].
func (IndexPartitioningService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	return reportSettings(ctx, request, ydbschema.IndexPartitioningKind, "ydb_index_partitioning", func(value schemaext.Value) (bool, error) {
		switch value := value.(type) {
		case *ydbschema.DesiredIndexPartitioning:
			if request.Representation == schemaext.Desired && value != nil {
				return !value.IsZero(), ydbschema.ValidateDesiredIndexPartitioning(value)
			}
		case *ydbschema.ObservedIndexPartitioning:
			if request.Representation == schemaext.Observed && value != nil {
				return !value.IsZero(), ydbschema.ValidateObservedIndexPartitioning(value)
			}
		}
		return false, fmt.Errorf("%w: YDB index partitioning report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, request.Representation)
	})
}

// reportSettings validates the representation and reports each value of kind
// once under metric where states says it states a setting. Errors and
// cancellation expose no partial report.
func reportSettings(ctx context.Context, request schemaext.ReportingRequest, kind schemaext.Kind, metric string,
	states func(schemaext.Value) (bool, error),
) ([]schemaext.ValueReport, error) {
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
		stated, err := states(value)
		if err != nil {
			return nil, err
		}
		count := 0
		if stated {
			count = 1
		}
		reports = append(reports, schemaext.ValueReport{Kind: kind, Counts: []schemaext.MetricCount{{Name: metric, Value: count}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

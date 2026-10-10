package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// VectorIndexService reports captured vector index settings without
// inspecting a server. Its zero value is usable and safe for concurrent calls.
type VectorIndexService struct{}

// VectorIndexDefinitions returns independent omission labels and count
// metadata. The count describes captured values, never the completeness of a
// catalog read.
func VectorIndexDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbschema.VectorIndexKind, DisplayName: "vector index settings", Metrics: []schemaext.MetricDefinition{
		{Name: "ydb_vector_indexes", Help: "YDB vector indexes with captured settings"},
	}}}
}

// ReportValues validates the representation and returns one ordered report per
// index value, counting each as one vector index. Errors and cancellation
// expose no partial report. Nil context or invalid values wrap
// schemaext.ErrInvalidValue; inputs remain unchanged.
func (VectorIndexService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reports := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := validReportedVector(value, request.Representation); err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: ydbschema.VectorIndexKind,
			Counts: []schemaext.MetricCount{{Name: "ydb_vector_indexes", Value: 1}}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

func validReportedVector(value schemaext.Value, representation schemaext.Representation) error {
	switch value := value.(type) {
	case *ydbschema.DesiredVectorIndex:
		if representation == schemaext.Desired {
			return ydbschema.ValidateDesiredVectorIndex(value)
		}
	case *ydbschema.ObservedVectorIndex:
		if representation == schemaext.Observed {
			return ydbschema.ValidateObservedVectorIndex(value)
		}
	}
	return fmt.Errorf("%w: YDB vector index report has mismatched value %T for %q", schemaext.ErrInvalidValue, value, representation)
}

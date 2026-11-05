package backend

import (
	"context"
	"fmt"
	"math"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// RetentionMetrics are the count metrics a program that lists
// provider.ReportCountsFeature declares on the retention models: each value
// is one retained object, and contributes the seconds it retains.
func RetentionMetrics() []*protocol.ModelMetric {
	return []*protocol.ModelMetric{
		{Name: "fixture_retained_objects", Help: "Objects with a captured fixture retention"},
		{Name: "fixture_retention_seconds", Help: "Seconds of fixture retention across objects"},
	}
}

// ReportValues counts retention values with RetentionMetrics: one object and
// its seconds, which a count larger than math.MaxInt64 holds at that bound.
func (*Provider) ReportValues(ctx context.Context, request *protocol.ReportValuesRequest) (*protocol.ValueReports, error) {
	result := &protocol.ValueReports{Complete: true}
	for _, input := range request.GetValues() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		seconds, err := retentionSeconds(input.GetValue().GetPayload(), input.GetRepresentation())
		if err != nil {
			return nil, fmt.Errorf("count retention: %w", err)
		}
		result.Values = append(result.Values, &protocol.ValueReport{Id: input.GetId(), Counts: []*protocol.MetricCount{
			{Name: "fixture_retained_objects", Value: 1}, {Name: "fixture_retention_seconds", Value: min(seconds, math.MaxInt64)},
		}})
	}
	return result, nil
}

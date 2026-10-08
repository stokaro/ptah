// Package ydbreport describes captured YDB feature values for inventory reports.
package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// Service counts named streams and their consumers without inspecting a server.
type Service struct{}

// Definitions returns independent reporting metadata for the changefeed model.
func Definitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{{Kind: ydbschema.ChangefeedKind, DisplayName: "changefeeds",
		Metrics: []schemaext.MetricDefinition{
			{Name: "changefeeds", Help: "Table changefeeds"},
			{Name: "changefeed_consumers", Help: "Consumers of table changefeeds"},
		}}}
}

// ReportValues preserves input order and distinguishes declaration values from
// observations. Disabled streams are still streams and contribute to the count.
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		spec, err := streamSpec(value, request.Representation)
		if err != nil {
			return nil, err
		}
		reports = append(reports, schemaext.ValueReport{Kind: ydbschema.ChangefeedKind, Counts: []schemaext.MetricCount{
			{Name: "changefeeds", Value: 1},
			{Name: "changefeed_consumers", Value: len(spec.Consumers)},
		}})
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return reports, nil
}

func streamSpec(value schemaext.Value, representation schemaext.Representation) (ydbschema.ChangefeedSpec, error) {
	if err := schemaext.ValidatePayload(value); err != nil {
		return ydbschema.ChangefeedSpec{}, err
	}
	var spec ydbschema.ChangefeedSpec
	switch representation {
	case schemaext.Desired:
		stream, ok := value.(*ydbschema.DesiredChangefeed)
		if !ok {
			return spec, fmt.Errorf("%w: expected desired changefeed, got %T", schemaext.ErrInvalidValue, value)
		}
		spec = stream.Spec
	case schemaext.Observed:
		stream, ok := value.(*ydbschema.ObservedChangefeed)
		if !ok {
			return spec, fmt.Errorf("%w: expected observed changefeed, got %T", schemaext.ErrInvalidValue, value)
		}
		spec = stream.Spec
	}
	if err := ydbschema.ValidateChangefeed(spec); err != nil {
		return ydbschema.ChangefeedSpec{}, err
	}
	return spec, nil
}

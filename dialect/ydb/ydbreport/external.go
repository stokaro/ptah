package ydbreport

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbexternal"
)

// ExternalService reports external data sources, external tables and the
// columns the tables declare.
type ExternalService struct{}

// ExternalDefinitions declares the inventory metrics for both external
// object kinds.
func ExternalDefinitions() []schemaext.ReportDefinition {
	return []schemaext.ReportDefinition{
		{Kind: ydbexternal.SourceKind, DisplayName: "external data sources",
			Metrics: []schemaext.MetricDefinition{{Name: "external_data_sources", Help: "External data sources"}}},
		{Kind: ydbexternal.TableKind, DisplayName: "external tables",
			Metrics: []schemaext.MetricDefinition{
				{Name: "external_tables", Help: "External tables"},
				{Name: "external_columns", Help: "Columns across external tables"},
			}},
	}
}

// ReportValues validates each value through its codec and counts the object,
// and an external table's columns.
func (ExternalService) ReportValues(ctx context.Context, request schemaext.ReportingRequest) ([]schemaext.ValueReport, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reporting requires a context", schemaext.ErrInvalidValue)
	}
	codecs := make(map[schemaext.Kind]schemaext.Codec)
	for _, candidate := range ydbexternal.Codecs() {
		if candidate.Representation == request.Representation {
			codecs[candidate.Prototype.Kind()] = candidate
		}
	}
	if len(codecs) == 0 {
		return nil, fmt.Errorf("%w: external object reporting requires desired or observed state", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.ValueReport, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if value == nil {
			return nil, fmt.Errorf("%w: external object reporting requires a value", schemaext.ErrInvalidValue)
		}
		codec, ok := codecs[value.Kind()]
		if !ok {
			return nil, fmt.Errorf("%w: unexpected external object %T", schemaext.ErrInvalidValue, value)
		}
		snapshot, err := codec.Clone(value)
		if err != nil {
			return nil, err
		}
		switch object := snapshot.(type) {
		case *ydbexternal.DesiredTable:
			result = append(result, tableReport(len(object.Spec.Columns)))
		case *ydbexternal.ObservedTable:
			result = append(result, tableReport(len(object.Spec.Columns)))
		default:
			result = append(result, schemaext.ValueReport{Kind: ydbexternal.SourceKind,
				Counts: []schemaext.MetricCount{{Name: "external_data_sources", Value: 1}}})
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func tableReport(columns int) schemaext.ValueReport {
	return schemaext.ValueReport{Kind: ydbexternal.TableKind, Counts: []schemaext.MetricCount{
		{Name: "external_tables", Value: 1}, {Name: "external_columns", Value: columns},
	}}
}

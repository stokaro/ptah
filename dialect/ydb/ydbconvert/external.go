package ydbconvert

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbexternal"
)

// ExternalService converts external data sources and external tables between
// a declaration and an observation. Every setting is kept as written: a path
// is not resolved against a database, and no default is filled in.
type ExternalService struct{}

// ConvertFeatures returns an independent batch, in input order.
func (ExternalService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	var sources, tables []int
	for i, value := range request.Values {
		if value == nil {
			return nil, fmt.Errorf("%w: external object conversion requires a value", schemaext.ErrInvalidValue)
		}
		switch value.Kind() {
		case ydbexternal.SourceKind:
			sources = append(sources, i)
		case ydbexternal.TableKind:
			tables = append(tables, i)
		default:
			return nil, fmt.Errorf("%w: unexpected external object conversion operand %T", schemaext.ErrInvalidValue, value)
		}
	}
	codecs := ydbexternal.Codecs()
	convertedSources, err := convertStandalone(ctx, subset(request, sources), "external data source", codecs[:2],
		(*ydbexternal.DesiredSource).Observed, (*ydbexternal.ObservedSource).Desired)
	if err != nil {
		return nil, err
	}
	convertedTables, err := convertStandalone(ctx, subset(request, tables), "external table", codecs[2:],
		(*ydbexternal.DesiredTable).Observed, (*ydbexternal.ObservedTable).Desired)
	if err != nil {
		return nil, err
	}
	values := make([]schemaext.Value, len(request.Values))
	for i, index := range sources {
		values[index] = convertedSources[i]
	}
	for i, index := range tables {
		values[index] = convertedTables[i]
	}
	return values, nil
}

// subset is request with only the values at indices.
func subset(request schemaext.ConversionRequest, indices []int) schemaext.ConversionRequest {
	values := make([]schemaext.Value, len(indices))
	for i, index := range indices {
		values[i] = request.Values[index]
	}
	request.Values = values
	return request
}

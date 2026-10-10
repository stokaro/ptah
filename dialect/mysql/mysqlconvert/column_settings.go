package mysqlconvert

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// ColumnService converts [mysqlschema.ColumnSettingsKind] values. An
// observation becomes the declaration that writes it exactly, the inherited
// character set included, so an exported column keeps the settings it was
// read with; a declaration becomes the observation a server reports for it.
type ColumnService struct{}

// ConvertFeatures converts a batch between the desired and the observed
// representation, in order. Another target, another direction and another
// value type are refused.
func (ColumnService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if !slices.Contains(mysqlschema.Targets(), request.Target) {
		return nil, fmt.Errorf("%w: MySQL conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid MySQL conversion direction", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convertColumnSettings(request.From, value)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, ctx.Err()
}

func convertColumnSettings(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	if from == schemaext.Desired {
		declared, ok := value.(*mysqlschema.DesiredColumnSettings)
		if !ok {
			return nil, fmt.Errorf("%w: expected desired MySQL column settings, got %T", schemaext.ErrInvalidValue, value)
		}
		if err := mysqlschema.ValidateDesiredColumnSettings(declared); err != nil {
			return nil, err
		}
		return declared.Observed(), nil
	}
	observed, ok := value.(*mysqlschema.ObservedColumnSettings)
	if !ok {
		return nil, fmt.Errorf("%w: expected observed MySQL column settings, got %T", schemaext.ErrInvalidValue, value)
	}
	if err := mysqlschema.ValidateObservedColumnSettings(observed); err != nil {
		return nil, err
	}
	return observed.Desired(), nil
}

package mysqlconvert

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// TableService converts table options. An observation becomes a declaration
// of the character set the table holds; a declaration becomes the options a
// read of a table created from it reports.
type TableService struct{}

// ConvertFeatures converts an ordered batch without mutating inputs. It accepts
// only the mysql and mariadb targets and opposite desired and observed
// representations. Invalid values or directions wrap
// schemaext.ErrInvalidValue; other targets wrap ptaherr.ErrUnsupportedDialect.
// Any error, including cancellation, returns no partial result.
func (TableService) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if request.Target != platform.MySQL && request.Target != platform.MariaDB {
		return nil, fmt.Errorf("%w: MySQL table options conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid MySQL table options conversion direction", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convertTable(request.From, value)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, ctx.Err()
}

func convertTable(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	if from == schemaext.Desired {
		desired, ok := value.(*mysqlschema.DesiredTable)
		if !ok {
			return nil, fmt.Errorf("%w: expected desired MySQL table options, got %T", schemaext.ErrInvalidValue, value)
		}
		return desired.Observed()
	}
	observed, ok := value.(*mysqlschema.ObservedTable)
	if !ok {
		return nil, fmt.Errorf("%w: expected observed MySQL table options, got %T", schemaext.ErrInvalidValue, value)
	}
	if err := mysqlschema.ValidateObservedTable(observed); err != nil {
		return nil, err
	}
	return observed.Desired(), nil
}

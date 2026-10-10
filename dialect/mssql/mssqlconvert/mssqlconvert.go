// Package mssqlconvert projects SQL Server security policies between their
// desired and observed representations. A projection is a prediction, never a
// claim that a server was inspected; source coverage travels separately.
package mssqlconvert

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
)

// Service converts security policy values. Its zero value is ready for
// concurrent use and never reads a database.
type Service struct{}

// ConvertFeatures converts an ordered batch without mutating its inputs. It
// accepts SQL Server and opposite desired and observed representations. A
// declaration converts with its defaults resolved, and an observation into a
// declaration that names every value. Any error, including cancellation,
// returns no partial result.
func (Service) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if platform.NormalizeDialect(request.Target) != platform.SQLServer {
		return nil, fmt.Errorf("%w: SQL Server security policy conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid security policy conversion direction", schemaext.ErrInvalidValue)
	}
	result := make([]schemaext.Value, 0, len(request.Values))
	for _, value := range request.Values {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		converted, err := convert(request.From, value)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	return result, ctx.Err()
}

func convert(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	switch typed := value.(type) {
	case *mssqlschema.DesiredSecurityPolicy:
		if from == schemaext.Desired {
			return typed.Observed()
		}
	case *mssqlschema.ObservedSecurityPolicy:
		if from == schemaext.Observed {
			return typed.Desired()
		}
	}
	return nil, fmt.Errorf("%w: unexpected %s security policy value %T", schemaext.ErrInvalidValue, from, value)
}

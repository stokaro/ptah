// Package typeconvert projects PostgreSQL domains, composite types and range
// types between their desired and observed representations for the owner of
// package pgtype. A projection is a prediction, never a claim that a server
// was inspected; source coverage travels separately.
package typeconvert

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// Service converts user types. Its zero value is ready for concurrent use and
// never reads a database.
type Service struct{}

// ConvertFeatures converts an ordered batch without mutating its inputs. It
// accepts PostgreSQL-family targets and opposite desired and observed
// representations. A declaration projects to the observation it predicts,
// such as [pgtype.DesiredDomain.Observed], and an observation to the
// declaration that keeps it as it is, such as [pgtype.ObservedDomain.Desired].
// Any error, including cancellation, returns no partial result.
func (Service) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return nil, fmt.Errorf("%w: PostgreSQL user type conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid user type conversion direction", schemaext.ErrInvalidValue)
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

// convert projects one value of the from representation, refusing a value of
// the other representation or of another model.
func convert(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	desired := from == schemaext.Desired
	switch typed := value.(type) {
	case *pgtype.DesiredDomain:
		if desired {
			return projected(typed.Observed(), pgtype.ValidateDesiredDomain(typed))
		}
	case *pgtype.DesiredComposite:
		if desired {
			return projected(typed.Observed(), pgtype.ValidateDesiredComposite(typed))
		}
	case *pgtype.DesiredRange:
		if desired {
			return projected(typed.Observed(), pgtype.ValidateDesiredRange(typed))
		}
	case *pgtype.ObservedDomain:
		if !desired {
			return projected(typed.Desired(), pgtype.ValidateObservedDomain(typed))
		}
	case *pgtype.ObservedComposite:
		if !desired {
			return projected(typed.Desired(), pgtype.ValidateObservedComposite(typed))
		}
	case *pgtype.ObservedRange:
		if !desired {
			return projected(typed.Desired(), pgtype.ValidateObservedRange(typed))
		}
	}
	return nil, fmt.Errorf("%w: unexpected %s user type value %T", schemaext.ErrInvalidValue, from, value)
}

// projected answers a projection of a value whose validation answered valid,
// or the validation's refusal and no value.
func projected(projection schemaext.Value, valid error) (schemaext.Value, error) {
	if valid != nil {
		return nil, valid
	}
	return projection, nil
}

// Package publicationconvert projects PostgreSQL publications between their
// desired and observed representations for the owner of package pgpublication.
// A projection is a prediction, never a claim that a server was inspected;
// source coverage travels separately.
package publicationconvert

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpublication"
)

// Service converts publications. Its zero value is ready for concurrent use and
// never reads a database.
type Service struct{}

// ConvertFeatures converts an ordered batch without mutating its inputs. It
// accepts the PostgreSQL target and opposite desired and observed
// representations. A declaration projects to the observation it predicts
// (see [pgpublication.DesiredPublication.Observed]), and an observation to the
// declaration that keeps it as it is (see
// [pgpublication.ObservedPublication.Desired]). Any error, including
// cancellation, returns no partial result.
func (Service) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if platform.NormalizeDialect(request.Target) != platform.Postgres {
		return nil, fmt.Errorf("%w: PostgreSQL publication conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid publication conversion direction", schemaext.ErrInvalidValue)
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
	case *pgpublication.DesiredPublication:
		if from == schemaext.Desired {
			if err := pgpublication.ValidateDesired(typed); err != nil {
				return nil, err
			}
			return typed.Observed(), nil
		}
	case *pgpublication.ObservedPublication:
		if from == schemaext.Observed {
			if err := pgpublication.ValidateObserved(typed); err != nil {
				return nil, err
			}
			return typed.Desired(), nil
		}
	}
	return nil, fmt.Errorf("%w: unexpected %s publication value %T", schemaext.ErrInvalidValue, from, value)
}

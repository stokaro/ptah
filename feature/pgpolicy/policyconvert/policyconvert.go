// Package policyconvert projects PostgreSQL row-security values between their
// desired and observed representations for the owner of package pgpolicy. A
// projection is a prediction, never a claim that a server was inspected;
// source coverage travels separately.
package policyconvert

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
)

// Service converts policies and table switches. Its zero value is ready for
// concurrent use and never reads a database.
type Service struct{}

// ConvertFeatures converts an ordered batch without mutating its inputs. It
// accepts PostgreSQL-family targets and opposite desired and observed
// representations. Any error, including cancellation, returns no partial
// result.
//
// A declaration projects with PostgreSQL's defaults resolved, and through the
// server's spelling where a probe attached one. A policy TO CURRENT_ROLE,
// CURRENT_USER or SESSION_USER without that spelling is refused: the catalog
// records the role the keyword resolved to when the policy was created, and
// no prediction can name it. An observation projects to the declaration that
// asks for exactly what it holds.
func (Service) ConvertFeatures(ctx context.Context, request schemaext.ConversionRequest) ([]schemaext.Value, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: conversion requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return nil, fmt.Errorf("%w: PostgreSQL row-security conversion on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if (request.From != schemaext.Desired && request.From != schemaext.Observed) ||
		(request.To != schemaext.Desired && request.To != schemaext.Observed) || request.From == request.To {
		return nil, fmt.Errorf("%w: invalid row-security conversion direction", schemaext.ErrInvalidValue)
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func convert(from schemaext.Representation, value schemaext.Value) (schemaext.Value, error) {
	switch typed := value.(type) {
	case *pgpolicy.DesiredPolicy:
		if from == schemaext.Desired {
			return typed.Observed()
		}
	case *pgpolicy.ObservedPolicy:
		if from == schemaext.Observed {
			return typed.Desired()
		}
	case *pgpolicy.DesiredTableState:
		if from == schemaext.Desired {
			return typed.Observed()
		}
	case *pgpolicy.ObservedTableState:
		if from == schemaext.Observed {
			return typed.Desired()
		}
	}
	return nil, fmt.Errorf("%w: unexpected %s row-security value %T", schemaext.ErrInvalidValue, from, value)
}

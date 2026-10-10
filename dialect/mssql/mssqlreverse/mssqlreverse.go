// Package mssqlreverse reverses captured SQL Server security policy changes
// and reports what restoring a policy cannot recover. It never reads a
// database.
package mssqlreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
)

// Service reconstructs the inverse of security policy changes from their
// complete operands. Its zero value is ready for concurrent use.
//
// The inverse swaps the operands: the forward declaration, predicted as the
// catalog reports it, becomes the observation, and the forward observation,
// with every value named, becomes the declaration. Its access assessment is
// computed again from those operands rather than inverted, since an unknown
// effect stays unknown in both directions.
type Service struct{}

// ReverseChanges preserves input order and returns no partial result on error
// or cancellation. ForwardState is a prediction, never inspection evidence.
func (Service) ReverseChanges(ctx context.Context, request schemaext.ReversalRequest) ([]schemaext.Reversal, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: reversal requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if platform.NormalizeDialect(request.Target) != platform.SQLServer {
		return nil, fmt.Errorf("%w: SQL Server security policy reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reversed, err := reverse(request, record)
		if err != nil {
			return nil, err
		}
		result = append(result, reversed)
	}
	return result, ctx.Err()
}

func reverse(request schemaext.ReversalRequest, record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	change, ok := record.Value.(*mssqldiff.SecurityPolicy)
	if !ok {
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a security policy change, got %T", schemaext.ErrInvalidValue, record.Value)
	}
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	if err := mssqlschema.ValidateSecurityPolicyRef(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	reversed := &mssqldiff.SecurityPolicy{}
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: mssqlschema.SecurityPolicyKind}
	if change.After != nil {
		after, err := change.After.Observed()
		if err != nil {
			return schemaext.Reversal{}, err
		}
		reversed.Before, projection.Value = after, after.Clone()
	}
	if change.Before != nil {
		before, err := change.Before.Desired()
		if err != nil {
			return schemaext.Reversal{}, err
		}
		reversed.After = before
	}
	reversed.Access = mssqldiff.Assess(request.Identifiers, reversed.Before, reversed.After)
	strategy := "restore the security policy's captured predicates and state"
	switch {
	case change.Before == nil:
		strategy = "drop the created security policy"
	case change.After == nil:
		strategy = "recreate the dropped security policy from its captured definition"
	}
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{projection}, Strategy: strategy,
		Limitations: []string{"Restoring a security policy does not undo the reads and writes it admitted while it was absent, disabled or different."},
	}, nil
}

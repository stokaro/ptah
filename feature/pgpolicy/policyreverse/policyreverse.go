// Package policyreverse reverses captured PostgreSQL row-security changes for
// the owner of package pgpolicy and reports what restoring a definition cannot
// recover. It never reads a database.
package policyreverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/normalize"
)

// accessLimitation is what every row-security reversal leaves as it was: who
// read or wrote which rows while the forward plan stood. ADR 0020 requires the
// reverse direction to keep saying so apart from whether the definition is
// recoverable.
const accessLimitation = "Restoring the previous row-security definition does not undo access the forward plan granted or withheld while it stood."

// Service reconstructs the inverse of policy and table-switch changes from
// their complete operands. Its zero value is ready for concurrent use.
//
// The inverse of a change holds the forward change's operands the other way
// round: the declaration it applied becomes the state the reverse starts from,
// projected as the server reports it, and the observation it replaced becomes
// the declaration the reverse applies. A policy TO a role keyword the server
// resolves when the policy is created has no projection without the server's
// answer, and the reversal is refused rather than guessed. The access each
// inverse can change is assessed again from its own operands.
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
	if !platform.IsPostgresFamily(request.Target) {
		return nil, fmt.Errorf("%w: PostgreSQL row-security reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	result := make([]schemaext.Reversal, 0, len(request.Changes))
	for _, record := range request.Changes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		reversed, err := reverse(record)
		if err != nil {
			return nil, err
		}
		result = append(result, reversed)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func reverse(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	switch change := record.Value.(type) {
	case *pgpolicy.PolicyChange:
		return reversePolicy(record, change)
	case *pgpolicy.TableStateChange:
		return reverseTableState(record, change)
	default:
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a row-security change, got %T", schemaext.ErrInvalidValue, record.Value)
	}
}

func reversePolicy(record schemaext.ChangeRecord, change *pgpolicy.PolicyChange) (schemaext.Reversal, error) {
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	if err := pgpolicy.ValidatePolicyRef(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	reversed := &pgpolicy.PolicyChange{CommentOnly: change.CommentOnly}
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: pgpolicy.PolicyKind}
	if change.After != nil {
		after, err := change.After.Observed()
		if err != nil {
			return schemaext.Reversal{}, fmt.Errorf("%w: %w", schemaext.ErrIrreversible, err)
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
	// The reversed operands hold the server's spelling on both sides wherever
	// the forward comparison had it, so a comment-only change compares equal
	// here too.
	expressions := pgpolicy.ExpressionsDiffer
	if reversed.Before != nil && reversed.After != nil && sameClause(reversed.Before.Using, reversed.After.Using) &&
		sameClause(reversed.Before.WithCheck, reversed.After.WithCheck) {
		expressions = pgpolicy.ExpressionsSame
	}
	reversed.Access = pgpolicy.PolicyAccess(reversed.Before, reversed.After, expressions)
	strategy := "replace the policy with its captured definition"
	switch {
	case change.CommentOnly:
		strategy = "restore the policy's captured comment"
	case change.Before == nil:
		strategy = "drop the created policy"
	case change.After == nil:
		strategy = "recreate the dropped policy from its captured definition"
	}
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{projection}, Strategy: strategy, Limitations: []string{accessLimitation}}, nil
}

func reverseTableState(record schemaext.ChangeRecord, change *pgpolicy.TableStateChange) (schemaext.Reversal, error) {
	if err := change.Validate(); err != nil {
		return schemaext.Reversal{}, err
	}
	after, err := change.After.Observed()
	if err != nil {
		return schemaext.Reversal{}, err
	}
	before, err := change.Before.Desired()
	if err != nil {
		return schemaext.Reversal{}, err
	}
	reversed := &pgpolicy.TableStateChange{Before: after, After: before, Access: pgpolicy.TableStateAccess(after, before)}
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: pgpolicy.TableStateKind, Value: after.Clone()}},
		Strategy:     "restore the table's captured row-security switches", Limitations: []string{accessLimitation}}, nil
}

// sameClause compares two clauses the way a comparison without a server's
// spelling does: present on both sides and equal after the textual fold, or
// absent on both.
func sameClause(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return normalize.Expression(*left) == normalize.Expression(*right)
}

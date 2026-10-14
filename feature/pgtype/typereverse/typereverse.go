// Package typereverse reverses captured PostgreSQL domain, composite type and
// range type changes for the owner of package pgtype, and reports what a
// rollback cannot recover. It never reads a database.
package typereverse

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
)

// Service reconstructs the inverse of user type changes from their operands.
// Its zero value is ready for concurrent use.
//
// The inverse of a creation drops the type, and the inverse of a drop creates
// it again as the catalog held it. A surviving type goes back to its
// observation: what the forward change found different differs again, in the
// other direction. A composite type's attributes go back in place where
// ALTER TYPE reaches the old list, and by a drop and a creation where it does
// not, as a forward change would.
//
// A domain's CHECK goes back by adding the constraint it had. The rollback
// cannot drop the constraint the migration added by name, because the server
// named it, and the inverse says so as a limitation.
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
		return nil, fmt.Errorf("%w: PostgreSQL user type reversal on %q", ptaherr.ErrUnsupportedDialect, request.Target)
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
	return result, ctx.Err()
}

func reverse(record schemaext.ChangeRecord) (schemaext.Reversal, error) {
	if err := pgtype.ValidateRef(record.Subject); err != nil {
		return schemaext.Reversal{}, err
	}
	switch change := record.Value.(type) {
	case *pgtype.DomainChange:
		return reverseDomain(record, change)
	case *pgtype.CompositeChange:
		return reverseComposite(record, change)
	case *pgtype.RangeChange:
		return reverseRange(record, change)
	default:
		return schemaext.Reversal{}, fmt.Errorf("%w: reversal requires a user type change, got %T", schemaext.ErrInvalidValue, record.Value)
	}
}

// valid refuses an invalid change and one of another model than its subject
// names.
func valid(record schemaext.ChangeRecord, model schemaext.Kind, validation error) error {
	if validation != nil {
		return validation
	}
	if schemaext.Kind(record.Subject.Kind) != model {
		return fmt.Errorf("%w: a %s change for %s", schemaext.ErrInvalidValue, model, record.Subject)
	}
	return nil
}

// reversal assembles one reversal: the forward state at the subject, which is
// absent after a drop, and the strategy the reverse change follows.
func reversal(record schemaext.ChangeRecord, reversed schemaext.ChangeValue, forward schemaext.Value, strategy string) schemaext.Reversal {
	projection := schemaext.ProjectedValue{Placement: schemaext.ObjectPlacement, Kind: schemaext.Kind(record.Subject.Kind), Value: forward}
	return schemaext.Reversal{Change: schemaext.ChangeRecord{Subject: record.Subject, Value: reversed},
		ForwardState: []schemaext.ProjectedValue{projection}, Strategy: strategy}
}

// strategies say what each direction of a reversal does, by which operand
// the forward change lacked.
func strategy(label string, before, after bool) string {
	switch {
	case !before:
		return "drop the " + label + " the forward plan created"
	case !after:
		return "create the " + label + " again as it was"
	default:
		return "change the " + label + " back to what it was"
	}
}

func reverseDomain(record schemaext.ChangeRecord, change *pgtype.DomainChange) (schemaext.Reversal, error) {
	if err := valid(record, pgtype.DomainKind, change.Validate()); err != nil {
		return schemaext.Reversal{}, err
	}
	label := pgtype.Label(record.Subject)
	if change.After == nil {
		return reversal(record, &pgtype.DomainChange{After: change.Before.Desired()}, nil, strategy(label, true, false)), nil
	}
	forward := change.After.Observed()
	if change.Before == nil {
		return reversal(record, &pgtype.DomainChange{Before: forward}, forward.Copy(), strategy(label, false, true)), nil
	}
	if !change.Differ(pgtype.DiffBaseType) {
		if change.Differ(pgtype.DiffCheck) {
			if replacement := change.ReplacementCheck(); replacement.Name != "" {
				forward.Checks = []pgtype.DomainCheck{replacement}
			}
		} else {
			forward.Check, forward.Checks = change.Before.Check, change.Before.Copy().Checks
		}
		if !change.Differ(pgtype.DiffDefault) {
			forward.Default = change.Before.Default
		}
	}
	return reversal(record, &pgtype.DomainChange{Before: forward, After: change.Before.Desired(), Differs: change.Differs},
		forward.Copy(), strategy(label, true, true)), nil
}

func reverseComposite(record schemaext.ChangeRecord, change *pgtype.CompositeChange) (schemaext.Reversal, error) {
	if err := valid(record, pgtype.CompositeKind, change.Validate()); err != nil {
		return schemaext.Reversal{}, err
	}
	label := pgtype.Label(record.Subject)
	if change.After == nil {
		return reversal(record, &pgtype.CompositeChange{After: change.Before.Desired()}, nil, strategy(label, true, false)), nil
	}
	forward := change.After.Observed()
	if change.Before == nil {
		return reversal(record, &pgtype.CompositeChange{Before: forward}, forward.Copy(), strategy(label, false, true)), nil
	}
	reversed := &pgtype.CompositeChange{Before: forward, After: change.Before.Desired(), Differs: change.Differs}
	if change.Differ(pgtype.DiffAttributes) {
		// The delta is simulated again over the old list: the attributes
		// the forward change appended may have been in the middle of it, and
		// ALTER TYPE appends.
		reversed.Added, reversed.Removed = pgtype.AttributeDelta(change.Before.Attributes, forward.Attributes)
	}
	return reversal(record, reversed, forward.Copy(), strategy(label, true, true)), nil
}

func reverseRange(record schemaext.ChangeRecord, change *pgtype.RangeChange) (schemaext.Reversal, error) {
	if err := valid(record, pgtype.RangeKind, change.Validate()); err != nil {
		return schemaext.Reversal{}, err
	}
	label := pgtype.Label(record.Subject)
	if change.After == nil {
		return reversal(record, &pgtype.RangeChange{After: change.Before.Desired()}, nil, strategy(label, true, false)), nil
	}
	forward := change.After.Observed()
	if change.Before == nil {
		return reversal(record, &pgtype.RangeChange{Before: forward}, forward.Copy(), strategy(label, false, true)), nil
	}
	return reversal(record, &pgtype.RangeChange{Before: forward, After: change.Before.Desired(), Differs: change.Differs},
		forward.Copy(), strategy(label, true, true)), nil
}

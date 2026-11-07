package providerhost

import (
	"context"
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/schemaprojection"
)

// constraintProjection predicts the table a provider engine's read reports
// after a change of the table's foreign keys, for the captured table a reverse
// plan starts from. An engine that negotiates observed constraints
// (provider.ObservedConstraintsFeature) reports each foreign key it holds as
// a constraint, and the index it builds to enforce one in neither the
// constraints nor the indexes of the table (ADR 0088), so the table after
// such a change is the table the change describes: no index or column
// differs. A change of any other kind, such as a primary key or a UNIQUE
// whose index a read reports, stays unavailable.
//
// Without it a reverse plan has no capture of a table it drops or alters
// after the forward plan adds a foreign key to it, and `migrations generate`
// of a schema with a foreign key fails on every provider engine.
type constraintProjection struct{}

// ProjectConstraints returns the table after the change, each foreign key
// the change adds in the spelling a read reports: its referenced schema and
// rules stated, an absent rule being NO ACTION.
func (constraintProjection) ProjectConstraints(ctx context.Context, request schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
	if ctx == nil {
		return schemaprojection.ConstraintResult{}, fmt.Errorf("%w: context is required", schemaprojection.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.ConstraintResult{}, err
	}
	for _, change := range request.Changes {
		for _, operand := range []*catalog.Constraint{change.Before, change.After} {
			if operand != nil && (!reportedConstraintType(operand.Type) || !operand.Facets.IsZero()) {
				return schemaprojection.ConstraintResult{Unavailable: fmt.Sprintf(
					"a provider read reports the effect of a %s constraint %s on its table's indexes, which the host cannot predict", operand.Type, operand.Name)}, nil
			}
		}
	}
	state := request.After.Clone()
	for i := range state.Constraints {
		constraint := &state.Constraints[i]
		if constraint.Type != "FOREIGN KEY" || !added(request, constraint.Name) {
			continue
		}
		if constraint.ForeignSchema == "" {
			constraint.ForeignSchema = request.Identifiers.DefaultSchema
		}
		if constraint.ForeignColumn == nil && len(constraint.ForeignColumns) > 0 {
			constraint.ForeignColumn = new(constraint.ForeignColumns[0])
		}
		if constraint.DeleteRule == nil {
			constraint.DeleteRule = new("NO ACTION")
		}
		if constraint.UpdateRule == nil {
			constraint.UpdateRule = new("NO ACTION")
		}
	}
	return schemaprojection.ConstraintResult{State: &state}, nil
}

// added reports whether a change of the request adds the constraint name.
func added(request schemaprojection.ConstraintRequest, name string) bool {
	for _, change := range request.Changes {
		if change.After != nil && request.Identifiers.IndexIdentityKey(change.After.Name) == request.Identifiers.IndexIdentityKey(name) {
			return true
		}
	}
	return false
}

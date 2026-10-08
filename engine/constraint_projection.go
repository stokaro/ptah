package engine

import (
	"context"
	"fmt"
	"reflect"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaprojection"
)

// ProjectConstraints invokes only the selected target's constraint projector.
// Missing support is an explicit unavailable prediction. Inputs and successful
// replies own their mutable data; malformed replies publish no partial state.
func (r *Runtime) ProjectConstraints(ctx context.Context, request schemaprojection.ConstraintRequest) (schemaprojection.ConstraintResult, error) {
	if ctx == nil {
		return schemaprojection.ConstraintResult{}, fmt.Errorf("%w: context is required", schemaprojection.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.ConstraintResult{}, err
	}
	target, found := r.lookup(request.Target)
	if !found {
		return schemaprojection.ConstraintResult{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request = request.Clone()
	request.Target = target.name
	if err := validateConstraintRequest(request); err != nil {
		return schemaprojection.ConstraintResult{}, err
	}
	if target.constraints == nil {
		return schemaprojection.ConstraintResult{Unavailable: "selected target has no constraint projection service"}, nil
	}
	// The validation ledger remains independent of the service's inputs.
	reply, err := target.constraints.ProjectConstraints(ctx, request.Clone())
	if err != nil {
		return schemaprojection.ConstraintResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.ConstraintResult{}, err
	}
	if err := reply.Validate(); err != nil {
		return schemaprojection.ConstraintResult{}, err
	}
	if reply.State == nil {
		return reply, nil
	}
	state := reply.State.Clone()
	if err := validateProjectedTable(request, state); err != nil {
		return schemaprojection.ConstraintResult{}, err
	}
	return schemaprojection.ConstraintResult{State: &state}, nil
}

func validateConstraintRequest(request schemaprojection.ConstraintRequest) error {
	if len(request.Changes) == 0 {
		return fmt.Errorf("%w: no accepted constraint changes", schemaprojection.ErrInvalid)
	}
	if err := validateProjectedTable(request, request.Before); err != nil {
		return err
	}
	if err := validateProjectedTable(request, request.After); err != nil {
		return err
	}
	builder := objectidentity.NewBuilder(request.Identifiers)
	parent := builder.TableParts(request.Before.Table.Schema, request.Before.Table.Name).Key()
	seen := make(map[objectidentity.Key]bool)
	for _, change := range request.Changes {
		var key objectidentity.Key
		for _, operand := range []*catalog.Constraint{change.Before, change.After} {
			if operand == nil {
				continue
			}
			if operand.Name == "" || builder.TableParts(operand.Schema, operand.TableName).Key() != parent {
				return fmt.Errorf("%w: constraint operand has no name or belongs to another table", schemaprojection.ErrInvalid)
			}
			identity := builder.ConstraintParts(operand.Schema, operand.TableName, operand.Name).Key()
			if key != (objectidentity.Key{}) && key != identity {
				return fmt.Errorf("%w: constraint replacement changes identity", schemaprojection.ErrInvalid)
			}
			key = identity
		}
		if key == (objectidentity.Key{}) || seen[key] {
			return fmt.Errorf("%w: absent or duplicate constraint transition", schemaprojection.ErrInvalid)
		}
		seen[key] = true
		if !matchesConstraintOperand(change.Before, request.Before.Constraints, key, builder) ||
			!matchesConstraintOperand(change.After, request.After.Constraints, key, builder) {
			return fmt.Errorf("%w: constraint transition disagrees with captured state", schemaprojection.ErrInvalid)
		}
	}
	return nil
}

func matchesConstraintOperand(operand *catalog.Constraint, constraints []catalog.Constraint, key objectidentity.Key, builder objectidentity.Builder) bool {
	for _, constraint := range constraints {
		if builder.ConstraintParts(constraint.Schema, constraint.TableName, constraint.Name).Key() == key {
			return operand != nil && reflect.DeepEqual(*operand, constraint)
		}
	}
	return operand == nil
}

func validateProjectedTable(request schemaprojection.ConstraintRequest, state schemaprojection.TableState) error {
	builder := objectidentity.NewBuilder(request.Identifiers)
	parent := builder.TableParts(request.Before.Table.Schema, request.Before.Table.Name).Key()
	if state.Table.Name == "" || builder.TableParts(state.Table.Schema, state.Table.Name).Key() != parent {
		return fmt.Errorf("%w: projected table identity differs from capture", schemaprojection.ErrInvalid)
	}
	seen := make(map[objectidentity.Key]bool)
	for _, index := range state.Indexes {
		key := builder.IndexParts(index.Schema, index.TableName, index.Name).Key()
		if index.Name == "" || builder.TableParts(index.Schema, index.TableName).Key() != parent || seen[key] {
			return fmt.Errorf("%w: missing, duplicate, or misowned projected index", schemaprojection.ErrInvalid)
		}
		seen[key] = true
	}
	clear(seen)
	for _, constraint := range state.Constraints {
		key := builder.ConstraintParts(constraint.Schema, constraint.TableName, constraint.Name).Key()
		if builder.TableParts(constraint.Schema, constraint.TableName).Key() != parent || (constraint.Name != "" && seen[key]) {
			return fmt.Errorf("%w: duplicate or misowned projected constraint", schemaprojection.ErrInvalid)
		}
		seen[key] = true
	}
	columns := make(map[string]bool)
	for _, column := range state.Table.Columns {
		key := request.Identifiers.ColumnIdentityKey(column.Name)
		if column.Name == "" || columns[key] {
			return fmt.Errorf("%w: missing or duplicate projected column", schemaprojection.ErrInvalid)
		}
		columns[key] = true
	}
	return nil
}

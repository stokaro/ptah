package generator

import (
	"context"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaprojection"
	"ptah.run/migration/schemadiff/difftypes"
)

func projectConstraintEffects(ctx context.Context, diff *difftypes.SchemaDiff, before, after schemacapture.TableObservation, dialect string, caps capability.Capabilities, runtime Runtime) (schemacapture.TableObservation, string, error) {
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	changes := constraintTransitions(diff, before, after, semantics)
	if len(changes) == 0 {
		return after, "", nil
	}
	reply, err := runtime.ProjectConstraints(ctx, schemaprojection.ConstraintRequest{
		Target: dialect, Identifiers: semantics, Capabilities: caps,
		Before: schemaprojection.TableState{Table: before.Table, Indexes: before.Indexes, Constraints: before.Constraints},
		After:  schemaprojection.TableState{Table: after.Table, Indexes: after.Indexes, Constraints: after.Constraints}, Changes: changes,
	})
	if err != nil {
		return schemacapture.TableObservation{}, "", err
	}
	if err := reply.Validate(); err != nil {
		return schemacapture.TableObservation{}, "", err
	}
	if reply.State == nil {
		return schemacapture.TableObservation{}, reply.Unavailable, nil
	}
	after.Table, after.Indexes, after.Constraints = reply.State.Table, reply.State.Indexes, reply.State.Constraints
	return after, "", nil
}

func constraintTransitions(diff *difftypes.SchemaDiff, before, after schemacapture.TableObservation, semantics identifier.Semantics) []schemaprojection.ConstraintChange {
	parent := semantics.QualifiedTableIdentityKey(before.Table.QualifiedName())
	var names []string
	seen := make(map[string]bool)
	selectName := func(table, name, kind string) {
		key := semantics.IndexIdentityKey(name)
		if kind != "CHECK" && semantics.QualifiedTableIdentityKey(table) == parent && !seen[key] {
			names = append(names, name)
			seen[key] = true
		}
	}
	for _, removed := range diff.ConstraintsRemoved {
		selectName(removed.TableName, removed.Name, removed.Type)
	}
	for _, added := range diff.ConstraintsAdded {
		selectName(added.TableName, added.Name, added.Type)
	}
	var changes []schemaprojection.ConstraintChange
	for _, name := range names {
		changes = append(changes, schemaprojection.ConstraintChange{
			Before: capturedConstraint(before.Constraints, name, semantics), After: capturedConstraint(after.Constraints, name, semantics),
		})
	}
	return changes
}

func capturedConstraint(constraints []catalog.Constraint, name string, semantics identifier.Semantics) *catalog.Constraint {
	for _, constraint := range constraints {
		if semantics.IndexIdentityKey(constraint.Name) == semantics.IndexIdentityKey(name) {
			return new(constraint.Clone())
		}
	}
	return nil
}

package schemachange

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemastate"
)

// compareTableConstraints compares the constraint kinds whose whole definition
// is one clause: CHECK, PRIMARY KEY and EXCLUDE.
//
// UNIQUE is not among them: it is a [schemastate.UniqueKey], because it answers
// the question a foreign key asks and these do not.
func compareTableConstraints(
	current, desired *schemastate.State,
	profile schemastate.Profile,
) []Change {
	changes := make([]Change, 0)
	declared := make(map[objectidentity.Key]struct{})

	for _, object := range desired.OfKind(objectidentity.KindConstraint) {
		if object.Constraint == nil {
			continue
		}
		declared[object.ID.Key()] = struct{}{}
		if !tableExists(current, object.Constraint.Table) {
			// The table is being created, and its constraints ride inside the
			// CREATE. Planning one here would declare it twice -- the same rule
			// a column of a new table follows.
			continue
		}
		existing, found := current.Get(object.ID)
		if !found || existing.Constraint == nil {
			changes = append(changes, decide(constraintAddition(object), profile, desired))
			continue
		}
		if changed := changedConstraint(*existing.Constraint, *object.Constraint); len(changed) > 0 {
			changes = append(changes,
				decide(constraintModification(object, existing, changed), profile, desired))
		}
	}

	for _, object := range current.OfKind(objectidentity.KindConstraint) {
		if object.Constraint == nil {
			continue
		}
		if _, wanted := declared[object.ID.Key()]; wanted {
			continue
		}
		if !tableExists(desired, object.Constraint.Table) {
			// The table is being dropped, and its constraints go with it.
			continue
		}
		changes = append(changes, decideConstraintRemoval(object, desired, profile, current))
	}
	return changes
}

// changedConstraint reports which parts of two constraints differ.
//
// The body is compared as the source wrote it, folded for whitespace and case
// only. Deciding that `price > 0` and `(price > 0)` are one condition is a
// TARGET's rule -- the server rewrites what it stores -- and it belongs to
// normalization rather than here, where a second copy of it would drift.
func changedConstraint(before, after schemastate.TableConstraint) []string {
	changed := make([]string, 0, 3)
	if !equalClause(before.Expression, after.Expression) {
		changed = append(changed, "expression")
	}
	if !slices.Equal(before.Columns, after.Columns) {
		changed = append(changed, "columns")
	}
	if !equalClause(before.UsingMethod, after.UsingMethod) ||
		!equalClause(before.Elements, after.Elements) ||
		!equalClause(before.Where, after.Where) {
		changed = append(changed, "exclusion")
	}
	return changed
}

func equalClause(before, after string) bool {
	return strings.EqualFold(strings.TrimSpace(before), strings.TrimSpace(after))
}

func constraintAddition(object schemastate.Object) Change {
	return Change{
		ID:        object.ID,
		Operation: Add,
		After:     &object,
		Evidence:  "declared by the desired schema and absent from the database",
		// Every one of these is validated against the rows already there, and
		// the plan cannot see rows.
		Risk:          RiskDataDependent,
		Reversibility: Reversible,
		Provenance:    object.Provenance,
	}
}

// constraintModification renders as a drop and an add, in that order, and stays
// ONE change: no engine alters any of these bodies in place, and the add cannot
// precede the drop while the name is taken.
func constraintModification(object, existing schemastate.Object, changed []string) Change {
	return Change{
		ID:            object.ID,
		Operation:     Modify,
		Before:        &existing,
		After:         &object,
		Changed:       changed,
		Evidence:      "both sides declare it and they disagree about " + joinChanged(changed),
		Risk:          RiskDataDependent,
		Reversibility: ReversibleWithData,
		Provenance:    object.Provenance,
	}
}

func decideConstraintRemoval(
	object schemastate.Object,
	desired *schemastate.State,
	profile schemastate.Profile,
	current *schemastate.State,
) Change {
	change := Change{
		ID:            object.ID,
		Operation:     Remove,
		Before:        &object,
		Evidence:      "present in the database and absent from the desired schema",
		Risk:          RiskGuaranteeLoss,
		Reversibility: ReversibleWithData,
		Provenance:    object.Provenance,
	}
	if !schemastate.DescribesTable(desired, object.ID) {
		change.Status = Undecidable
		change.Diagnostic = fmt.Sprintf(
			"%s is not dropped: the desired schema declares schema %q not-described, so its silence "+
				"about this constraint is not a request to drop it",
			change, object.ID.Schema.Source)
		return change
	}
	return decide(change, profile, current)
}

// tableConstraintNodes renders one clause constraint's change.
func tableConstraintNodes(change Change, profile schemastate.Profile) ([]ast.Node, error) {
	switch change.Operation {
	case Add:
		return []ast.Node{addConstraintNode(change)}, nil
	case Remove:
		return []ast.Node{dropConstraintNode(change, profile)}, nil
	case Modify:
		return []ast.Node{dropConstraintNode(change, profile), addConstraintNode(change)}, nil
	default:
		return nil, fmt.Errorf("%s: unknown operation %q", change, change.Operation)
	}
}

func addConstraintNode(change Change) ast.Node {
	constraint := change.After.Constraint
	return &ast.AlterTableNode{
		Name: qualify(constraint.Table.Schema, constraint.Table.Name),
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{
			Constraint: constraintNode(*constraint),
		}},
	}
}

// excludeConstraintKind is the one clause kind that is enforced with an index.
const excludeConstraintKind = "EXCLUDE"

// constraintKind folds a kind to the spelling the arms below are written in.
// The field carries whatever the SOURCE spelled, and the two sources do not
// have to agree with each other about case or padding.
func constraintKind(constraint schemastate.TableConstraint) string {
	return strings.ToUpper(strings.TrimSpace(constraint.Kind))
}

// constraintNode builds the clause for one kind.
func constraintNode(constraint schemastate.TableConstraint) *ast.ConstraintNode {
	switch constraintKind(constraint) {
	case "PRIMARY KEY":
		// Named with whatever the source called it, which for a declaration is
		// nothing: `ADD PRIMARY KEY (...)` lets the server derive the name, and
		// that is what the shipping planner emits.
		return ast.NewPrimaryKeyConstraint(constraint.Columns...)
	case excludeConstraintKind:
		return excludeConstraintNode(constraint)
	default:
		return &ast.ConstraintNode{
			Type:       ast.CheckConstraint,
			Name:       constraint.ConstraintName,
			Expression: constraint.Expression,
		}
	}
}

func excludeConstraintNode(constraint schemastate.TableConstraint) *ast.ConstraintNode {
	node := ast.NewExcludeConstraint(
		constraint.ConstraintName, constraint.UsingMethod, constraint.Elements)
	return withWhereCondition(node, constraint.Where)
}

func withWhereCondition(node *ast.ConstraintNode, where string) *ast.ConstraintNode {
	return map[bool]func() *ast.ConstraintNode{
		true:  func() *ast.ConstraintNode { return node },
		false: func() *ast.ConstraintNode { return node.SetWhereCondition(where) },
	}[strings.TrimSpace(where) == ""]()
}

func dropConstraintNode(change Change, profile schemastate.Profile) ast.Node {
	constraint := change.Before.Constraint
	return &ast.AlterTableNode{
		Name: qualify(constraint.Table.Schema, constraint.Table.Name),
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			// The name the SERVER holds it under, which for a primary key is
			// not the identity this comparison uses.
			ConstraintName: constraint.ConstraintName,
			IfExists:       supportsIfExists(profile),
		}},
	}
}

// tableExists reports whether a state describes the table a constraint is on.
//
// It is what separates "this constraint changed" from "the table it is on is
// being created or dropped", and the second is not this family's statement to
// write: a constraint of a table that does not exist yet rides inside the
// CREATE, and one whose table is going away goes with it.
func tableExists(state *schemastate.State, table objectidentity.ID) bool {
	object, found := state.Get(table)
	return found && object.Table != nil
}

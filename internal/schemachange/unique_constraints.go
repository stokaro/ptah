package schemachange

import (
	"fmt"
	"slices"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemastate"
)

// compareUniqueConstraints compares the uniqueness guarantees the target holds
// as objects of their own.
//
// Only a STANDALONE guarantee produces a change. A column's own UNIQUE flag
// renders beside its column, so planning it here would declare the same
// guarantee twice; a primary key and a unique index are added and dropped by
// statements of their own, with their own risk, and each is its own slice. All
// three are still objects, because all three answer the question a foreign key
// asks (stokaro/ptah#1663).
func compareUniqueConstraints(
	current, desired *schemastate.State,
	profile schemastate.Profile,
) []Change {
	changes := make([]Change, 0)
	declared := make(map[objectidentity.Key]struct{})

	for _, object := range desired.OfKind(objectidentity.KindConstraint) {
		if !standaloneUniqueKey(object) {
			continue
		}
		declared[object.ID.Key()] = struct{}{}
		existing, found := current.Get(object.ID)
		if !found || existing.UniqueKey == nil {
			changes = append(changes, decide(uniqueAddition(object), profile, desired))
			continue
		}
		if changedColumns(*existing.UniqueKey, *object.UniqueKey, desired) {
			changes = append(changes,
				decide(uniqueModification(object, existing), profile, desired))
		}
	}

	for _, object := range current.OfKind(objectidentity.KindConstraint) {
		if !standaloneUniqueKey(object) {
			continue
		}
		if _, wanted := declared[object.ID.Key()]; wanted {
			continue
		}
		changes = append(changes, decideUniqueRemoval(object, desired, profile, current))
	}
	return changes
}

// standaloneUniqueKey reports whether an object is a uniqueness guarantee this
// comparison plans.
func standaloneUniqueKey(object schemastate.Object) bool {
	return object.UniqueKey != nil && object.UniqueKey.Standalone
}

// changedColumns reports whether two guarantees cover different columns.
//
// The comparison is over the SET, for the reason [schemastate.UniqueKey.Covers]
// gives: a guarantee on (a, b) and one on (b, a) make the same rows unique, and
// no engine distinguishes them. The ORDER still matters to what is rendered --
// the index behind the constraint is built in it -- but re-ordering an existing
// constraint is a rebuild nobody asked for, so a difference in order alone is
// not a change.
func changedColumns(before, after schemastate.UniqueKey, side *schemastate.State) bool {
	fold := side.Profile().Semantics.TableIdentityKey
	return !after.Covers(before.Columns, fold)
}

// uniqueAddition describes a guarantee the desired schema declares and the
// database does not have.
func uniqueAddition(object schemastate.Object) Change {
	return Change{
		ID:        object.ID,
		Operation: Add,
		After:     &object,
		Evidence:  "declared by the desired schema and absent from the database",
		// Adding one fails on rows that already violate it, and the plan cannot
		// see rows. That is a property of the change, not of the statement.
		Risk:          RiskDataDependent,
		Reversibility: Reversible,
		Provenance:    object.Provenance,
	}
}

// uniqueModification describes a guarantee both sides have over different
// columns. No engine Ptah targets alters a constraint's column list in place,
// so it renders as a drop and an add -- and stays ONE change, because a later
// stage that saw only one half could order them apart.
func uniqueModification(object, existing schemastate.Object) Change {
	return Change{
		ID:            object.ID,
		Operation:     Modify,
		Before:        &existing,
		After:         &object,
		Changed:       []string{"columns"},
		Evidence:      "both sides declare it and they disagree about which columns it covers",
		Risk:          RiskDataDependent,
		Reversibility: ReversibleWithData,
		Provenance:    object.Provenance,
	}
}

// decideUniqueRemoval describes a guarantee the database has and the desired
// schema does not declare.
//
// Dropping one destroys a guarantee rather than data, and re-adding it can fail
// on rows that accumulated while it was gone -- the shape a foreign-key removal
// already has.
func decideUniqueRemoval(
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

// uniqueConstraintNodes renders one guarantee's change.
//
// A modification is a DROP and then an ADD, in that order, and it stays ONE
// change carrying both. The order is not a preference: measured on PostgreSQL
// 17, adding a constraint whose name is already taken fails with
// `relation "uq_widget_scope" already exists`, because the backing index
// carries the name too.
func uniqueConstraintNodes(change Change, profile schemastate.Profile) ([]ast.Node, error) {
	switch change.Operation {
	case Add:
		return []ast.Node{addUniqueNode(change)}, nil
	case Remove:
		return []ast.Node{dropUniqueNode(change, profile)}, nil
	case Modify:
		return []ast.Node{dropUniqueNode(change, profile), addUniqueNode(change)}, nil
	default:
		return nil, fmt.Errorf("%s: unknown operation %q", change, change.Operation)
	}
}

func addUniqueNode(change Change) ast.Node {
	return &ast.AlterTableNode{
		Name: qualify(change.ID.Schema, change.ID.Parent),
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{
			Constraint: ast.NewUniqueConstraint(
				change.ID.Name.Source, slices.Clone(change.After.UniqueKey.Columns)...),
		}},
	}
}

func dropUniqueNode(change Change, profile schemastate.Profile) ast.Node {
	return &ast.AlterTableNode{
		Name: qualify(change.ID.Schema, change.ID.Parent),
		Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
			ConstraintName: change.ID.Name.Source,
			IfExists:       supportsIfExists(profile),
		}},
	}
}

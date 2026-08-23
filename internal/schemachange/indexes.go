package schemachange

import (
	"fmt"
	"slices"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemastate"
)

// compareIndexes compares the index family.
//
// An index is not a constraint that happens to have columns: its identity is
// scoped by the target rather than by the model, and the adapters build it
// through [objectidentity.Builder.Index] for exactly that reason. This
// comparison therefore does nothing about namespacing -- by the time an object
// is here, two indexes are one object or two because the TARGET says so
// (stokaro/ptah#1663).
func compareIndexes(current, desired *schemastate.State, profile schemastate.Profile) []Change {
	changes := make([]Change, 0)
	declared := make(map[objectidentity.Key]struct{})

	for _, object := range desired.OfKind(objectidentity.KindIndex) {
		if object.Index == nil {
			continue
		}
		declared[object.ID.Key()] = struct{}{}
		existing, found := current.Get(object.ID)
		if !found || existing.Index == nil {
			changes = append(changes, decide(indexAddition(object), profile, desired))
			continue
		}
		if changed := changedIndexProperties(*existing.Index, *object.Index); len(changed) > 0 {
			changes = append(changes,
				decide(indexModification(object, existing, changed), profile, desired))
		}
	}

	for _, object := range current.OfKind(objectidentity.KindIndex) {
		if object.Index == nil {
			continue
		}
		if _, wanted := declared[object.ID.Key()]; wanted {
			continue
		}
		if backsAConstraint(current, object, profile.Dialect) {
			continue
		}
		changes = append(changes, decideIndexRemoval(object, desired, profile, current))
	}
	return changes
}

// backsAConstraint reports whether a database index is the index a constraint
// is enforced with, rather than an index of its own.
//
// PostgreSQL, MySQL and MariaDB enforce a UNIQUE constraint with an index of
// the constraint's own name on the constraint's own table, and introspection
// reports that ONE object twice: once in the index catalog and once in the
// constraint catalog. On MySQL and MariaDB there is not even a separate notion
// to report -- `ADD CONSTRAINT uq UNIQUE (email)` and `CREATE UNIQUE INDEX uq`
// produce the same row. MySQL and MariaDB create one for every FOREIGN KEY as
// well, named after the constraint, which a schema written the ordinary way
// never asked for and never mentions.
//
// Reading either as an object of its own made a desired state that declares the
// CONSTRAINT look like one that dropped the INDEX, and the plan came out as
//
//	DROP INDEX IF EXISTS "public"."uq_widget_code"
//
// which drops the constraint with it. Measured against the shipping comparator,
// which plans nothing for either pair (stokaro/ptah#1663, stokaro/ptah#1286,
// stokaro/ptah#1245, stokaro/ptah#1258).
//
// The question is asked of the CURRENT state only. An index the desired state
// declares is handled by the loop above -- it is in `declared`, so it never
// reaches here -- which is what keeps a schema that really does want a
// standalone unique index from having its index suppressed by the constraint
// the server created for it. **Which representation wins is decided by the
// description, not by the dialect.**
//
// An index no statement addresses at all -- a PRIMARY KEY's, and the one SQLite
// names for itself -- is not a state object in the first place; see
// [go.5x5.cz/ptah/internal/schemastate.FromCatalog].
func backsAConstraint(current *schemastate.State, index schemastate.Object, dialect string) bool {
	for _, object := range current.OfKind(objectidentity.KindConstraint) {
		if !ownsAnIndex(object, dialect) {
			continue
		}
		if sameOwnedName(object.ID, index.ID, index.Index.Table) {
			return true
		}
	}
	return false
}

// ownsAnIndex reports whether a constraint of this kind is enforced with an
// index of its own name on this target.
//
// SQL Server keeps a UNIQUE constraint and a unique index as separate objects,
// so there is nothing to hand over there. Only MySQL and MariaDB create an
// index for a FOREIGN KEY; on the other targets an index sharing a foreign
// key's name is an index somebody wrote.
func ownsAnIndex(constraint schemastate.Object, dialect string) bool {
	switch {
	case constraint.UniqueKey != nil && constraint.UniqueKey.Standalone:
		return platform.NormalizeDialect(dialect) != platform.SQLServer
	case constraint.ForeignKey != nil:
		normalized := platform.NormalizeDialect(dialect)
		return normalized == platform.MySQL || normalized == platform.MariaDB
	default:
		return false
	}
}

// sameOwnedName reports whether a constraint and an index name one object: the
// same name on the same table.
//
// The index's table comes from its payload rather than its identity, because a
// target that scopes index names to a schema leaves the table out of the
// identity entirely -- and this comparison is precisely about the table they
// share.
func sameOwnedName(constraint, index, indexTable objectidentity.ID) bool {
	return constraint.Name.Normalized == index.Name.Normalized &&
		constraint.Parent.Normalized == indexTable.Name.Normalized &&
		constraint.Schema.Normalized == indexTable.Schema.Normalized
}

// changedIndexProperties reports which properties of two indexes differ.
//
// A key the reader could not fully name reports NOTHING. Its Columns are a
// prefix of the real key -- a MySQL functional key part has no column name in
// the catalog -- so comparing them against a declaration would plan a rebuild
// on every run for a key that never changed, and the rebuild would drop the
// part the reader could not see.
func changedIndexProperties(before, after schemastate.Index) []string {
	if before.KeyPartsIncomplete {
		return nil
	}
	changed := make([]string, 0, 2)
	if !slices.Equal(before.Columns, after.Columns) {
		changed = append(changed, "columns")
	}
	if before.Unique != after.Unique {
		changed = append(changed, "uniqueness")
	}
	return changed
}

// indexAddition describes an index the desired schema declares and the database
// does not have.
func indexAddition(object schemastate.Object) Change {
	return Change{
		ID:        object.ID,
		Operation: Add,
		After:     &object,
		Evidence:  "declared by the desired schema and absent from the database",
		// Building one reads every row of the table and, for a unique index,
		// fails on the first duplicate. Neither is visible to the plan.
		Risk:          RiskDataDependent,
		Reversibility: Reversible,
		RequiredFacts: schemastate.IndexRequiredFacts(*object.Index),
		Provenance:    object.Provenance,
	}
}

// indexModification describes an index both sides have and disagree about.
//
// No engine Ptah targets alters an index's key in place, so it renders as a
// drop and an add, and it stays ONE change for the reason a changed constraint
// does: a later stage that saw only one half could order them apart, and the
// add cannot precede the drop while the name is taken.
func indexModification(object, existing schemastate.Object, changed []string) Change {
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

// decideIndexRemoval describes an index the database has and the desired schema
// does not declare.
//
// Dropping one destroys no data. What it costs is the queries that depended on
// it, which the plan cannot see, and rebuilding it reads the whole table again.
func decideIndexRemoval(
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
				"about this index is not a request to drop it",
			change, object.ID.Schema.Source)
		return change
	}
	return decide(change, profile, current)
}

// indexNodes renders one index change.
func indexNodes(change Change) ([]ast.Node, error) {
	switch change.Operation {
	case Add:
		return []ast.Node{addIndexNode(change)}, nil
	case Remove:
		return []ast.Node{dropIndexNode(change)}, nil
	case Modify:
		return []ast.Node{dropIndexNode(change), addIndexNode(change)}, nil
	default:
		return nil, fmt.Errorf("%s: unknown operation %q", change, change.Operation)
	}
}

func addIndexNode(change Change) ast.Node {
	return &ast.IndexNode{
		Name:    change.ID.Name.Source,
		Table:   indexTableName(change.After.Index.Table),
		Columns: slices.Clone(change.After.Index.Columns),
		Unique:  change.After.Index.Unique,
		// The request travels to the renderer, which is where the target's
		// grammar lives. The planner does not decide whether the target can do
		// it -- the change already named that as a required fact, and a target
		// without it never reaches here.
		Concurrently: change.After.Index.Concurrent,
		// Guarded, which is what the shipping planner emits: a CREATE INDEX
		// replayed against a database that already has it is not a failure
		// anybody wants, and the index is the same either way.
		IfNotExists: true,
	}
}

func dropIndexNode(change Change) ast.Node {
	return &ast.DropIndexNode{
		Name:  change.ID.Name.Source,
		Table: indexTableName(change.Before.Index.Table),
		// Same guard, same reason.
		IfExists: true,
	}
}

// indexTableName is the table an index statement names, qualified the way the
// side it came from spelled it.
//
// It reads the table off the PAYLOAD and not off the identity, because a target
// that scopes index names to a schema leaves the owning table out of the
// identity entirely -- and `CREATE INDEX ... ON <table>` still needs one.
func indexTableName(table objectidentity.ID) string {
	return qualify(table.Schema, table.Name)
}

// joinChanged renders a property list for an evidence sentence.
func joinChanged(changed []string) string {
	out := ""
	for index, property := range changed {
		out = appendProperty(out, property, index)
	}
	return out
}

func appendProperty(out, property string, index int) string {
	return map[bool]string{
		true:  property,
		false: out + ", " + property,
	}[index == 0]
}

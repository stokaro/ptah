package schemachange

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/internal/schemastate"
	"go.5x5.cz/ptah/internal/typechange"
)

// compareTables compares the table family and, for the tables both sides have,
// the columns inside them.
//
// A table one side creates carries its columns with it. There is no separate
// column change for a table that does not exist yet, because the column is not
// a separate statement: it is part of the CREATE, and splitting it would let a
// later stage order the two apart. The same holds in reverse for a drop. Only a
// table both sides have produces column changes (stokaro/ptah#1662).
func compareTables(current, desired *schemastate.State, profile schemastate.Profile) ([]Change, error) {
	for _, kind := range []objectidentity.Kind{objectidentity.KindTable, objectidentity.KindColumn} {
		if err := schemastate.RequireScope(desired, kind); err != nil {
			return nil, fmt.Errorf("the desired schema: %w", err)
		}
	}
	changes := make([]Change, 0)
	declared := make(map[objectidentity.Key]struct{})

	for _, object := range desired.OfKind(objectidentity.KindTable) {
		if object.Table == nil {
			continue
		}
		declared[object.ID.Key()] = struct{}{}
		existing, found := current.Get(object.ID)
		if !found || existing.Table == nil {
			changes = append(changes, decideTableAddition(object, current, profile, desired))
			continue
		}
		changes = append(changes, compareColumns(existing, object, profile, desired)...)
	}

	for _, object := range current.OfKind(objectidentity.KindTable) {
		if object.Table == nil {
			continue
		}
		if _, wanted := declared[object.ID.Key()]; wanted {
			continue
		}
		changes = append(changes, decideTableRemoval(object, desired, profile, current))
	}
	return changes, nil
}

// decideTableAddition plans a table the desired schema declares and the read
// did not report.
//
// The CURRENT side's coverage gates it. A read that never opened the schema
// says nothing about what is in it, and `CREATE TABLE` has no conditional form
// on any dialect Ptah renders, so planning one for a table that is already
// there fails the migration. The change is reported as undecidable rather than
// dropped: a plan that quietly omits something the author declared is worse
// than one that says it could not tell (stokaro/ptah#1276).
func decideTableAddition(
	object schemastate.Object,
	current *schemastate.State,
	profile schemastate.Profile,
	desired *schemastate.State,
) Change {
	change := Change{
		ID:        object.ID,
		Operation: Add,
		After:     &object,
		Evidence:  "declared by the desired schema and absent from the database",
		// Creating a table costs nothing that existed before it, and dropping
		// it undoes the creation exactly.
		Risk:          RiskLow,
		Reversibility: Reversible,
		Provenance:    object.Provenance,
	}
	if !schemastate.DescribesTable(current, object.ID) {
		change.Status = Undecidable
		change.Diagnostic = fmt.Sprintf(
			"%s is not created: the database read declares schema %q not-described, so this comparison "+
				"cannot tell a table that is missing from one it never looked for, and CREATE TABLE "+
				"has no conditional form that converges from an unknown current state",
			change, object.ID.Schema.Source)
		return change
	}
	return decide(change, profile, desired)
}

// decideTableRemoval plans a table the database has and the desired schema does
// not declare.
//
// It is the one change in this family that cannot be undone. Dropping a table
// destroys its rows, and re-creating it produces an empty table of the same
// shape rather than the table that was there, so the change says Irreversible
// instead of being handed a rollback nobody can execute.
func decideTableRemoval(
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
		Risk:          RiskDataLoss,
		Reversibility: Irreversible,
		Provenance:    object.Provenance,
	}
	if !schemastate.DescribesTable(desired, object.ID) {
		change.Status = Undecidable
		change.Diagnostic = fmt.Sprintf(
			"%s is not dropped: the desired schema declares schema %q not-described, so its silence "+
				"about this table is not a request to drop it",
			change, object.ID.Schema.Source)
		return change
	}
	return decide(change, profile, current)
}

// compareColumns compares the columns of one table both sides carry.
func compareColumns(
	existing, object schemastate.Object,
	profile schemastate.Profile,
	desired *schemastate.State,
) []Change {
	changes := make([]Change, 0)
	declared := make(map[objectidentity.Key]struct{})

	for _, column := range object.Table.Columns {
		declared[column.ID.Key()] = struct{}{}
		before, found := existing.Table.Column(column.ID)
		if !found {
			changes = append(changes, decideColumnAddition(object, existing, column, profile, desired))
			continue
		}
		if changed := changedColumnProperties(before, column, profile); len(changed) > 0 {
			changes = append(changes,
				decide(columnModification(object, before, column, changed), profile, desired))
		}
	}

	for _, column := range existing.Table.Columns {
		if _, wanted := declared[column.ID.Key()]; wanted {
			continue
		}
		changes = append(changes, decide(columnRemoval(object, existing, column), profile, desired))
	}
	return changes
}

// decideColumnAddition plans a column the desired schema declares on a table
// that already exists.
//
// A NOT NULL column with nothing to fill it is the case this exists for. The
// statement succeeds on an empty table and fails on every row of a populated
// one, and the failure arrives at apply time with the migration half applied.
// The plan can do better than that, because the catalog read already carries
// the table's row statistics:
//
//   - the table is estimated to hold rows: BLOCKED, with the estimate named.
//     The target cannot host this change as written, and the fix is a default
//     or a nullable column.
//   - the estimate is zero and statistics exist: PLANNED. The best evidence
//     available says the table is empty.
//   - the server keeps no usable statistics: UNDECIDABLE. That is a
//     measurement nobody has taken, not an empty table, and the two must not
//     produce the same plan.
func decideColumnAddition(
	object, existing schemastate.Object,
	column schemastate.Column,
	profile schemastate.Profile,
	desired *schemastate.State,
) Change {
	change := Change{
		ID:            column.ID,
		Operation:     Add,
		After:         &schemastate.Object{ID: column.ID, Column: &column, Provenance: object.Provenance},
		Evidence:      "declared by the desired schema and absent from the table the database reports",
		Risk:          RiskLow,
		Reversibility: Reversible,
		Provenance:    object.Provenance,
	}
	if column.Nullable || column.Supplied() {
		return decide(change, profile, desired)
	}
	// Adding it fails on any row that exists, so the risk is a property of the
	// data rather than of the statement.
	change.Risk = RiskDataDependent
	populated, known := existing.Table.Populated()
	switch {
	case !known:
		change.Status = Undecidable
		change.Diagnostic = fmt.Sprintf(
			"%s is not added: it is NOT NULL with nothing to fill it, and the database reports no usable "+
				"row statistics for %s, so this comparison cannot tell an empty table from one whose rows "+
				"would fail the statement",
			change, object.ID)
		return change
	case populated:
		change.Status = Blocked
		change.Diagnostic = fmt.Sprintf(
			"%s is not added: it is NOT NULL with nothing to fill it, and %s is estimated to hold %d rows, "+
				"each of which would have no value for it -- declare a default or make the column nullable",
			change, object.ID, existing.Table.EstimatedRows)
		return change
	default:
		change.Evidence = "declared by the desired schema, absent from the table, and the database " +
			"reports the table as empty"
		return decide(change, profile, desired)
	}
}

// columnModification describes a column both sides carry and disagree about.
//
// Reverting one is expressible and its success depends on data the plan cannot
// see: narrowing a type truncates, and widening it back returns the type
// without the bytes. That is exactly [ReversibleWithData], and claiming plain
// reversibility would attach a rollback that silently does less than it says.
func columnModification(
	object schemastate.Object,
	before, after schemastate.Column,
	changed []string,
) Change {
	return Change{
		ID:        after.ID,
		Operation: Modify,
		Before: &schemastate.Object{
			ID: before.ID, Column: &before, Provenance: object.Provenance,
		},
		After: &schemastate.Object{
			ID: after.ID, Column: &after, Provenance: object.Provenance,
		},
		Changed:       changed,
		Evidence:      "both sides declare it and they disagree about " + strings.Join(changed, ", "),
		Risk:          RiskDataDependent,
		Reversibility: ReversibleWithData,
		Provenance:    object.Provenance,
	}
}

// columnRemoval describes a column the database has and the desired schema does
// not declare. Dropping it destroys what was in it, so it is irreversible for
// the reason a table drop is.
//
// The identity is rebuilt on the DESIRED table's spelling. The column itself
// exists only on the catalog side, so its identity carries the catalog's
// qualification -- and the statement this renders to alters a table BOTH sides
// describe, which the author spelled their own way. Writing the catalog's
// spelling back would qualify a table an author wrote bare, which is the same
// mistake as emitting a defaulted schema (ADR 0001 invariant 2).
func columnRemoval(object, existing schemastate.Object, column schemastate.Column) Change {
	id := objectidentity.ID{
		Kind:   objectidentity.KindColumn,
		Schema: object.ID.Schema,
		Parent: object.ID.Name,
		Name:   column.ID.Name,
	}
	return Change{
		ID:            id,
		Operation:     Remove,
		Before:        &schemastate.Object{ID: id, Column: &column, Provenance: existing.Provenance},
		Evidence:      "present in the database and absent from the desired schema",
		Risk:          RiskDataLoss,
		Reversibility: Irreversible,
		Provenance:    existing.Provenance,
	}
}

// changedColumnProperties reports which properties of two columns differ, in a
// fixed order so a diagnostic reads the same twice.
func changedColumnProperties(before, after schemastate.Column, profile schemastate.Profile) []string {
	changed := make([]string, 0, 3)
	if typeChanged(before, after, profile) {
		changed = append(changed, "type")
	}
	if before.Nullable != after.Nullable {
		changed = append(changed, "nullability")
	}
	if before.HasDefault != after.HasDefault || before.Default != after.Default {
		changed = append(changed, "default")
	}
	// Both sources report a generated column's expression and kind, so this is
	// a comparison rather than a carry: a column that starts or stops being
	// generated is a change either side can ask for. Check and CheckName are
	// NOT here, for the reason schemastate.Column records -- a catalog reports
	// a column-level check as a table-level constraint row.
	if before.GeneratedExpression != after.GeneratedExpression ||
		before.GeneratedKind != after.GeneratedKind {
		changed = append(changed, "generated expression")
	}
	return changed
}

// typeChanged reports whether two columns declare different types, in the two
// steps the shipping comparator uses.
//
// The folded types decide first. Deciding that a declared `int` and a catalog
// `integer` are one type is a TARGET's rule, and it belongs to normalization,
// which is where the referential-action default already lives and which fills
// [schemastate.Column.TypeNormalized] out of the same two packages
// migration/schemadiff reads.
//
// The fold is deliberately lossy about WIDTH, and that is the second step. It
// strips the size, so `varchar(50)` and `varchar(100)` fold to one string --
// which is right for SQLite, where affinity means a width is not a type
// distinction at all, and wrong everywhere else: narrowing can lose data and
// widening is still an ALTER a database built from the declaration would carry.
// So a same-fold pair is asked again, about the sizes the fold discarded.
func typeChanged(before, after schemastate.Column, profile schemastate.Profile) bool {
	if isDomain(before) || isDomain(after) {
		return domainChanged(before, after, profile)
	}
	if !strings.EqualFold(before.TypeNormalized, after.TypeNormalized) {
		return true
	}
	if platform.NormalizeDialect(profile.Dialect) == platform.SQLite {
		return false
	}
	return typechange.IsNarrowing(before.Type, after.Type) || typechange.IsWidening(before.Type, after.Type)
}

// isDomain reports whether a column's declared type is a domain.
func isDomain(column schemastate.Column) bool {
	return strings.TrimSpace(column.DomainName) != ""
}

// domainChanged compares two columns when either declares a domain.
//
// A domain is compared by IDENTITY and never through the type fold. Its name is
// an identifier its author chose, and a domain may be named after a type:
// measured, a column whose catalog type is the domain `int8` against a
// declaration of the base type `bigint` folded to one string on both sides and
// read as unchanged, which is the ALTER that never happens
// (stokaro/ptah#1138, stokaro/ptah#1662).
//
// A domain on one side and a base type on the other is therefore always a
// change, whatever the two spellings fold to.
func domainChanged(before, after schemastate.Column, profile schemastate.Profile) bool {
	// A domain on one side and a base type on the other is caught by the name
	// comparison below: a column that is not a domain has no domain name, and
	// no domain has an empty one. Guarding it separately would be a branch no
	// test could tell from that one.
	fold := profile.Semantics.TableIdentityKey
	if fold(before.DomainName) != fold(after.DomainName) {
		return true
	}
	// An empty schema on either side is "not said, and nothing here can resolve
	// it" -- two domains of one name in different schemas are ambiguous, and
	// the adapters record that by leaving the schema off rather than picking
	// one. Comparing an unresolved schema against a resolved one would report a
	// change nobody can act on.
	if before.DomainSchema == "" || after.DomainSchema == "" {
		return false
	}
	return fold(before.DomainSchema) != fold(after.DomainSchema)
}

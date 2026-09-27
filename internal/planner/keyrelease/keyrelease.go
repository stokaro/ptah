// Package keyrelease finds the keys a migration plan drops before a column of
// the same table takes its own UNIQUE, because each holds the name the server
// gives that key.
//
// The MySQL-family and PostgreSQL planners add keys before they drop the keys
// the desired state leaves out. That order keeps a key a foreign key needs in
// place while its replacement is built. A removed key that holds the name of
// the column's own key breaks it. On MySQL and MariaDB the server names the
// column's key `x_2`, and the next comparison plans it again. On PostgreSQL
// `ADD CONSTRAINT c_x_key` answers that the name exists. Atlas CE v1.3.0 drops
// the holder first. So a planner drops each key this package finds ahead of the
// column changes, and skips it where the plan's removals land.
//
// The name is the one [columnkey.Name] gives with no other key taken, the
// first the server tries. Where a declared key holds it, the column's key takes
// a later name, and a removed key that holds that one is dropped with the other
// removals; the plan then takes a second comparison to settle the name.
package keyrelease

import (
	"slices"
	"strings"

	"ptah.run/internal/columnkey"
	"ptah.run/internal/constraintscope"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/tableref"
	"ptah.run/migration/schemadiff/difftypes"
)

// Releases are the removals a plan runs before its column changes, in the
// order the diff lists them.
type Releases struct {
	// Constraints are removed constraints.
	Constraints []difftypes.ConstraintRemovalInfo
	// Indexes are removed indexes.
	Indexes []difftypes.IndexRef
}

// ConstraintSet keys the released constraints by identity.
func (r Releases) ConstraintSet() map[difftypes.ConstraintIdentity]struct{} {
	set := make(map[difftypes.ConstraintIdentity]struct{}, len(r.Constraints))
	for _, info := range r.Constraints {
		set[info.Identity] = struct{}{}
	}
	return set
}

// IndexSet keys the released indexes.
func (r Releases) IndexSet() map[difftypes.IndexRef]struct{} {
	set := make(map[difftypes.IndexRef]struct{}, len(r.Indexes))
	for _, ref := range r.Indexes {
		set[ref] = struct{}{}
	}
	return set
}

// Find answers the removals in diff that hold the name a column's own UNIQUE
// takes on dialect. The columns are those of a modified table that gain one: a
// column added with UNIQUE, and a column whose uniqueness changes to UNIQUE. A
// column declaring unique_expr takes no key over the raw column and is left
// out.
//
// A constraint is released when it is on the column's table and holds a name
// the key cannot share there; see [columnkey.Shares]. An index is released when
// it is on the column's table. A removal the plan adds again is a modification,
// which drops and adds the object itself, and is never released. An engine
// [columnkey.Named] does not name gives none.
func Find(diff *difftypes.SchemaDiff, dialect string) Releases {
	if diff == nil || !columnkey.Named(dialect) {
		return Releases{}
	}
	takes := keyNames(diff, dialect)
	if takes == nil {
		return Releases{}
	}
	return Releases{
		Constraints: releasedConstraints(diff, dialect, takes),
		Indexes:     releasedIndexes(diff, dialect, takes),
	}
}

// keyNames answers whether a name on a table is one a column of that table
// gains its own UNIQUE under, and nil where no column gains one.
func keyNames(diff *difftypes.SchemaDiff, dialect string) func(table, name string) bool {
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	names := make(map[string][]string)
	for _, table := range diff.TablesModified {
		key := semantics.QualifiedTableIdentityKey(table.TableName)
		for _, column := range gainingColumns(table) {
			name, _ := columnkey.Name(dialect, bareTableName(table.TableName), column, nil)
			names[key] = append(names[key], name)
		}
	}
	if len(names) == 0 {
		return nil
	}
	return func(table, name string) bool {
		return slices.ContainsFunc(names[semantics.QualifiedTableIdentityKey(table)], func(keyName string) bool {
			return columnkey.Same(dialect, keyName, name)
		})
	}
}

// releasedConstraints answers the removed constraints of diff that hold a name
// takes reports, and that no addition of diff puts back.
func releasedConstraints(
	diff *difftypes.SchemaDiff,
	dialect string,
	takes func(table, name string) bool,
) []difftypes.ConstraintRemovalInfo {
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	// A diff the comparison built carries each identity; one built by hand
	// does not, and the planners fill it the same way before they read it.
	identity := func(table, name string, carried difftypes.ConstraintIdentity) difftypes.ConstraintIdentity {
		if carried == (difftypes.ConstraintIdentity{}) {
			return constraintscope.Identity(semantics, table, name)
		}
		return carried
	}
	added := make(map[difftypes.ConstraintIdentity]struct{}, len(diff.ConstraintsAdded))
	for _, add := range diff.ConstraintsAdded {
		added[identity(add.TableName, add.Name, add.Identity)] = struct{}{}
	}
	addedNames := make(map[string]struct{})
	for _, name := range constraintscope.AdditionNames(diff) {
		addedNames[semantics.IndexIdentityKey(name)] = struct{}{}
	}
	var released []difftypes.ConstraintRemovalInfo
	for _, info := range diff.ConstraintsRemoved {
		if info.TableName == "" || !columnkey.Shares(dialect, info.Type) || !takes(info.TableName, info.Name) {
			continue
		}
		info.Identity = identity(info.TableName, info.Name, info.Identity)
		_, modified := added[info.Identity]
		_, readded := addedNames[semantics.IndexIdentityKey(info.Name)]
		if !modified && !readded {
			released = append(released, info)
		}
	}
	return released
}

// releasedIndexes answers the removed indexes of diff that hold a name takes
// reports, and that no addition of diff rebuilds.
func releasedIndexes(diff *difftypes.SchemaDiff, dialect string, takes func(table, name string) bool) []difftypes.IndexRef {
	replacements := indexscope.NewConflictSetWithSemantics(diff.EffectiveIdentifierSemantics(dialect), diff.IndexAdditions())
	rebuilt := diff.IndexRemovalsRebuiltAsUniqueConstraints()
	var released []difftypes.IndexRef
	for _, ref := range diff.IndexRemovals() {
		_, rebuiltAsConstraint := rebuilt[ref]
		if !replacements.Contains(ref) && !rebuiltAsConstraint && takes(ref.TableName, ref.Name) {
			released = append(released, ref)
		}
	}
	return released
}

// gainingColumns answers the columns of table that gain their own UNIQUE.
func gainingColumns(table difftypes.TableDiff) []string {
	var columns []string
	for _, field := range table.ColumnsAdded {
		if field.Unique && strings.TrimSpace(field.UniqueExpr) == "" {
			columns = append(columns, field.Name)
		}
	}
	for _, column := range table.ColumnsModified {
		_, changed := column.Changes["unique"]
		if changed && column.Desired.Unique && strings.TrimSpace(column.Desired.UniqueExpr) == "" {
			columns = append(columns, column.ColumnName)
		}
	}
	return columns
}

// bareTableName is the relation name of table, without its schema.
func bareTableName(table string) string {
	if ref, ok := tableref.Parse(table); ok {
		return ref.Name
	}
	return table
}

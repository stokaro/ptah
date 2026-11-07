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
// The name is the one the comparison derived from the names the desired state
// holds, which the diff carries; see [difftypes.TableDiff.ColumnKeyNames]. A
// diff built by hand carries none, and the name [columnkey.Name] gives with no
// other key taken, the first the server tries, stands in for it.
package keyrelease

import (
	"slices"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/columnkey"
	"ptah.run/internal/constraintscope"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/tableref"
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

// NamesRenamedKeysTake answers the removed indexes that hold a name a renamed primary
// key takes. A key's index and any other index of its schema share one namespace, so
// the rename answers `relation "x" already exists` while the index is there, and the
// plan's removals come after it.
func NamesRenamedKeysTake(diff *difftypes.SchemaDiff, dialect string) Releases {
	if diff == nil {
		return Releases{}
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	schemaOf := func(table string) string {
		if ref, ok := tableref.Parse(table); ok && ref.Schema != "" {
			return semantics.TableIdentityKey(ref.Schema)
		}
		return semantics.TableIdentityKey(semantics.DefaultSchema)
	}
	var released Releases
	for _, add := range diff.ConstraintsAdded {
		if add.RenamedFrom == "" {
			continue
		}
		for _, ref := range diff.IndexRemovals() {
			if semantics.IndexIdentityKey(ref.Name) == semantics.IndexIdentityKey(add.Name) && schemaOf(ref.TableName) == schemaOf(add.TableName) {
				released.Indexes = append(released.Indexes, ref)
			}
		}
	}
	return released
}

// UnderAddedForeignKeys answers the removals in diff that a foreign key the plan
// adds could otherwise bind to, on a server where a foreign key depends on one
// index and keeps it; see
// [ptah.run/core/platform/descriptor.Comparison.ForeignKeyHoldsItsKey].
//
// A planner adds foreign keys before it drops the keys the desired state leaves
// out. The server binds the new foreign key to the oldest index over its columns,
// which is the removed one while it is still there, so the drop that follows
// answers `cannot drop constraint parent_code_old on table parent because other
// objects depend on it`. These go ahead of the additions instead: every removed
// PRIMARY KEY and UNIQUE constraint and every removed index of a table such a
// foreign key references. A removal the plan adds again, and a primary key the plan
// replaces, is already dropped ahead of its addition, and is left out.
func UnderAddedForeignKeys(diff *difftypes.SchemaDiff, dialect string) Releases {
	if diff == nil {
		return Releases{}
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	referenced := make(map[string]bool)
	gainsPrimaryKey := make(map[string]bool)
	added := make(map[difftypes.ConstraintIdentity]bool)
	for _, add := range diff.ConstraintsAdded {
		added[constraintscope.Identity(semantics, add.TableName, add.Name)] = true
		switch {
		case strings.EqualFold(add.Type, "FOREIGN KEY"):
			referenced[semantics.QualifiedTableIdentityKey(add.ForeignTable)] = true
		case strings.EqualFold(add.Type, "PRIMARY KEY"):
			gainsPrimaryKey[semantics.QualifiedTableIdentityKey(add.TableName)] = true
		}
	}
	if len(referenced) == 0 {
		return Releases{}
	}
	var released Releases
	for _, info := range diff.ConstraintsRemoved {
		table := semantics.QualifiedTableIdentityKey(info.TableName)
		primaryKey := strings.EqualFold(info.Type, "PRIMARY KEY")
		if !referenced[table] || (!primaryKey && !strings.EqualFold(info.Type, "UNIQUE")) ||
			(primaryKey && gainsPrimaryKey[table]) || added[constraintscope.Identity(semantics, info.TableName, info.Name)] {
			continue
		}
		info.Identity = constraintscope.Identity(semantics, info.TableName, info.Name)
		released.Constraints = append(released.Constraints, info)
	}
	replacements := indexscope.NewConflictSetWithSemantics(semantics, diff.IndexAdditions())
	rebuilt := diff.IndexRemovalsRebuiltAsUniqueConstraints()
	for _, ref := range diff.IndexRemovals() {
		_, rebuiltAsConstraint := rebuilt[ref]
		if referenced[semantics.QualifiedTableIdentityKey(ref.TableName)] && !replacements.Contains(ref) && !rebuiltAsConstraint {
			released.Indexes = append(released.Indexes, ref)
		}
	}
	return released
}

// UnderColumnDrops answers the removed UNIQUE and CHECK constraints and the removed
// indexes of each table that loses a column, which a planner that drops columns ahead of
// its other removals drops ahead of the column changes. A key or an index over only that
// column leaves with it, so its own drop then answers `Error 1091: Can't DROP`, and a
// CHECK over only that column answers `Error 3940: Constraint does not exist`. An index
// over that column and another one stays, narrowed, which can answer a duplicate key on
// a UNIQUE. Measured on MySQL 8.4. MariaDB takes the drop under a guard, and the order
// costs it nothing. The comparison records a removed object by its name alone, so every
// such object of the table moves.
//
// The key [DroppedWithColumns] finds stays out: it leaves with its column, and no
// statement is written for it. A constraint the plan adds back moves too, since its drop
// only moves ahead of the column changes, and an index a UNIQUE constraint rebuilds does
// the same. An index rebuilt under its own name stays out: a foreign key may need it at
// every moment, so a planner moves its one statement instead. So does an index an
// addition takes over from, which leaves in the statement of an addition; see
// [difftypes.IndexChange.Replaces].
func UnderColumnDrops(diff *difftypes.SchemaDiff, dialect string) Releases {
	if diff == nil {
		return Releases{}
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	losing := TablesLosingColumns(diff, semantics)
	if len(losing) == 0 {
		return Releases{}
	}
	var released Releases
	takenWithColumn := DroppedWithColumns(diff, dialect)
	takenOver := takenOverByAdditions(diff, semantics)
	for _, info := range diff.ConstraintsRemoved {
		if !losing[semantics.QualifiedTableIdentityKey(info.TableName)] ||
			(!strings.EqualFold(info.Type, "UNIQUE") && !strings.EqualFold(info.Type, "CHECK")) {
			continue
		}
		info.Identity = constraintscope.Identity(semantics, info.TableName, info.Name)
		_, taken := takenWithColumn[info.Identity]
		_, over := takenOver[info.Identity]
		if !taken && !over {
			released.Constraints = append(released.Constraints, info)
		}
	}
	replacements := indexscope.NewConflictSetWithSemantics(semantics, diff.IndexAdditions())
	for _, ref := range diff.IndexRemovals() {
		_, over := takenOver[constraintscope.Identity(semantics, ref.TableName, ref.Name)]
		if losing[semantics.QualifiedTableIdentityKey(ref.TableName)] && !replacements.Contains(ref) && !over {
			released.Indexes = append(released.Indexes, ref)
		}
	}
	return released
}

// UniquesTakenOver keys the removed UNIQUE constraints an addition of the plan takes over
// from; see [difftypes.IndexChange.Replaces]. A planner writes each one in the statement
// of such an addition, so its constraint removals leave it out. A foreign key can hold
// the name of an index taken over and still has to leave, so only a UNIQUE counts.
func UniquesTakenOver(diff *difftypes.SchemaDiff, dialect string) map[difftypes.ConstraintIdentity]struct{} {
	if diff == nil {
		return nil
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	replaced := takenOverByAdditions(diff, semantics)
	taken := make(map[difftypes.ConstraintIdentity]struct{})
	for _, info := range diff.ConstraintsRemoved {
		identity := constraintscope.Identity(semantics, info.TableName, info.Name)
		if _, over := replaced[identity]; over && strings.EqualFold(info.Type, "UNIQUE") {
			taken[identity] = struct{}{}
		}
	}
	return taken
}

// takenOverByAdditions keys everything an addition of the plan takes over from; see
// [difftypes.SchemaDiff.IndexesTakenOver].
func takenOverByAdditions(diff *difftypes.SchemaDiff, semantics identifier.Semantics) map[difftypes.ConstraintIdentity]struct{} {
	taken := make(map[difftypes.ConstraintIdentity]struct{})
	for ref := range diff.IndexesTakenOver() {
		taken[constraintscope.Identity(semantics, ref.TableName, ref.Name)] = struct{}{}
	}
	return taken
}

// TablesLosingColumns keys the tables of diff that lose a column.
func TablesLosingColumns(diff *difftypes.SchemaDiff, semantics identifier.Semantics) map[string]bool {
	losing := make(map[string]bool)
	for _, table := range diff.TablesModified {
		if len(table.ColumnsRemoved) > 0 {
			losing[semantics.QualifiedTableIdentityKey(table.TableName)] = true
		}
	}
	return losing
}

// NamesIndexesTake answers the removed UNIQUE constraints of diff under the name of an
// index the plan adds on their table. A UNIQUE constraint is an index of its name, so
// `CREATE INDEX u` answers `Error 1061: Duplicate key name 'u'` on MySQL and MariaDB and
// `relation "u" already exists` on PostgreSQL while the constraint stands, and a planner
// adds indexes ahead of its constraint removals. A rollback of an index turned into a
// UNIQUE under one name has this shape.
func NamesIndexesTake(diff *difftypes.SchemaDiff, dialect string) Releases {
	if diff == nil || len(diff.IndexesAdded) == 0 {
		return Releases{}
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	added := indexscope.NewConflictSetWithSemantics(semantics, diff.IndexAdditions())
	var released Releases
	for _, info := range diff.ConstraintsRemoved {
		ref := difftypes.IndexRef{Name: info.Name, TableName: info.TableName}
		if strings.EqualFold(info.Type, "UNIQUE") && added.Contains(ref) {
			info.Identity = constraintscope.Identity(semantics, info.TableName, info.Name)
			released.Constraints = append(released.Constraints, info)
		}
	}
	return released
}

// PrimaryKeysUnderColumnChanges answers the removed primary keys of diff that a
// column change of their own table cannot run under, which a planner drops ahead of
// the column changes. On PostgreSQL, measured on 18.6, `ALTER COLUMN id DROP NOT NULL`
// answers `column "id" is in a primary key` while the key holds the column, and
// `ADD COLUMN code bigint PRIMARY KEY` answers `multiple primary keys for table
// "parent" are not allowed` while the table holds one. On MySQL and MariaDB the first
// statement runs and the column stays NOT NULL, so the next comparison plans it again.
// A primary key the plan adds back is one of these too: its drop only moves ahead of
// the column changes.
func PrimaryKeysUnderColumnChanges(diff *difftypes.SchemaDiff, dialect string) Releases {
	if diff == nil {
		return Releases{}
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	held := make(map[string]bool)
	for _, table := range diff.TablesModified {
		if columnChangesRequireNoPrimaryKey(table) {
			held[semantics.QualifiedTableIdentityKey(table.TableName)] = true
		}
	}
	var released Releases
	for _, info := range diff.ConstraintsRemoved {
		if strings.EqualFold(info.Type, "PRIMARY KEY") && held[semantics.QualifiedTableIdentityKey(info.TableName)] {
			info.Identity = constraintscope.Identity(semantics, info.TableName, info.Name)
			released.Constraints = append(released.Constraints, info)
		}
	}
	return released
}

// PrimaryKeysBehindAutoIncrement answers the removed primary keys of diff whose column
// gives up AUTO_INCREMENT and the key at once. MySQL and MariaDB refuse to drop a key
// under a column that still has AUTO_INCREMENT, with Error 1075, and a newer MySQL
// refuses a nullable column in a key the table holds, with Error 1171. So a planner
// takes AUTO_INCREMENT off first and drops the key before the column allows NULL.
func PrimaryKeysBehindAutoIncrement(diff *difftypes.SchemaDiff, dialect string) Releases {
	if diff == nil {
		return Releases{}
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	held := make(map[string]bool)
	for _, table := range diff.TablesModified {
		if slices.ContainsFunc(table.ColumnsModified, GivesUpAutoIncrementKey) {
			held[semantics.QualifiedTableIdentityKey(table.TableName)] = true
		}
	}
	var released Releases
	for _, info := range diff.ConstraintsRemoved {
		if strings.EqualFold(info.Type, "PRIMARY KEY") && held[semantics.QualifiedTableIdentityKey(info.TableName)] {
			info.Identity = constraintscope.Identity(semantics, info.TableName, info.Name)
			released.Constraints = append(released.Constraints, info)
		}
	}
	return released
}

// GivesUpAutoIncrementKey reports a column that stops being AUTO_INCREMENT and the
// table's key in one change.
func GivesUpAutoIncrementKey(column difftypes.ColumnDiff) bool {
	return column.Changes["auto_increment"] == "true -> false" && column.Changes["primary_key"] == "true -> false"
}

// columnChangesRequireNoPrimaryKey reports whether table adds a column that declares
// a primary key, or changes a column of its key to allow NULL.
func columnChangesRequireNoPrimaryKey(table difftypes.TableDiff) bool {
	if slices.ContainsFunc(table.ColumnsAdded, func(field schemamodel.Field) bool { return field.Primary }) {
		return true
	}
	return slices.ContainsFunc(table.ColumnsModified, func(column difftypes.ColumnDiff) bool {
		// MySQL refuses to drop the key under a column that still has AUTO_INCREMENT,
		// so a column giving up both stays behind its own MODIFY COLUMN.
		_, autoIncrement := column.Changes["auto_increment"]
		return !autoIncrement && strings.HasPrefix(column.Changes["nullable"], "false ->") && strings.HasPrefix(column.Changes["primary_key"], "true ->")
	})
}

// With is r and the removals of other that r does not hold, in that order.
func (r Releases) With(other Releases) Releases {
	constraints, indexes := r.ConstraintSet(), r.IndexSet()
	merged := Releases{Constraints: slices.Clone(r.Constraints), Indexes: slices.Clone(r.Indexes)}
	for _, info := range other.Constraints {
		if _, held := constraints[info.Identity]; !held {
			merged.Constraints = append(merged.Constraints, info)
		}
	}
	for _, ref := range other.Indexes {
		if _, held := indexes[ref]; !held {
			merged.Indexes = append(merged.Indexes, ref)
		}
	}
	return merged
}

// TakenBy answers the removals of r on table that hold the name the own UNIQUE of
// column takes on dialect, the name [Find] reads. A planner that writes them in the
// statement giving column that UNIQUE frees the name and takes it at once, which
// leaves a foreign key standing on one of them an index at every moment.
func (r Releases) TakenBy(diff *difftypes.SchemaDiff, dialect string, table difftypes.TableDiff, column string) Releases {
	if diff == nil || !columnkey.Named(dialect) {
		return Releases{}
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	name := ownKeyName(dialect, table, column)
	on := func(other string) bool {
		return semantics.QualifiedTableIdentityKey(other) == semantics.QualifiedTableIdentityKey(table.TableName)
	}
	var taken Releases
	for _, info := range r.Constraints {
		if on(info.TableName) && columnkey.Same(dialect, name, info.Name) {
			taken.Constraints = append(taken.Constraints, info)
		}
	}
	for _, ref := range r.Indexes {
		if on(ref.TableName) && columnkey.Same(dialect, name, ref.Name) {
			taken.Indexes = append(taken.Indexes, ref)
		}
	}
	return taken
}

// Without is r less the removals other holds, in r's order.
func (r Releases) Without(other Releases) Releases {
	constraints, indexes := other.ConstraintSet(), other.IndexSet()
	var kept Releases
	for _, info := range r.Constraints {
		if _, held := constraints[info.Identity]; !held {
			kept.Constraints = append(kept.Constraints, info)
		}
	}
	for _, ref := range r.Indexes {
		if _, held := indexes[ref]; !held {
			kept.Indexes = append(kept.Indexes, ref)
		}
	}
	return kept
}

// ownKeyName is the name the own UNIQUE of column on table takes on dialect: the one
// the comparison derived, or the first the server tries where the diff carries none.
func ownKeyName(dialect string, table difftypes.TableDiff, column string) string {
	if name := table.ColumnKeyNames[column]; name != "" {
		return name
	}
	name, _ := columnkey.Name(dialect, bareTableName(table.TableName), column, nil)
	return name
}

// keyNames answers whether a name on a table is one a column of that table
// gains its own UNIQUE under, and nil where no column gains one.
func keyNames(diff *difftypes.SchemaDiff, dialect string) func(table, name string) bool {
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	names := make(map[string][]string)
	for _, table := range diff.TablesModified {
		key := semantics.QualifiedTableIdentityKey(table.TableName)
		for _, column := range gainingColumns(table) {
			names[key] = append(names[key], ownKeyName(dialect, table, column))
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

// DroppedWithColumns answers the removed UNIQUE keys of diff that are the own
// key of a column the same plan drops. The server drops that key with the
// column. The key is matched by the name [columnkey.Name] gives it with no
// other key taken, the name the server tries first, and an engine [columnkey.Named]
// does not name gives none.
//
// A planner that removes constraints after it drops columns skips each key this
// finds, where its server takes no guard on the drop. Measured on MySQL 26.10,
// `ALTER TABLE c DROP COLUMN x` and then `ALTER TABLE c DROP INDEX x` answers
// `Can't DROP 'x'; check that column/key exists`.
func DroppedWithColumns(diff *difftypes.SchemaDiff, dialect string) map[difftypes.ConstraintIdentity]struct{} {
	if diff == nil || !columnkey.Named(dialect) {
		return nil
	}
	semantics := diff.EffectiveIdentifierSemantics(dialect)
	held := make(map[string][]string)
	for _, table := range diff.TablesModified {
		key := semantics.QualifiedTableIdentityKey(table.TableName)
		for _, field := range table.ColumnsRemoved {
			if !field.Unique || strings.TrimSpace(field.UniqueExpr) != "" {
				continue
			}
			if name, named := columnkey.Name(dialect, bareTableName(table.TableName), field.Name, nil); named {
				held[key] = append(held[key], name)
			}
		}
	}
	var dropped map[difftypes.ConstraintIdentity]struct{}
	for _, info := range diff.ConstraintsRemoved {
		if info.TableName == "" || !strings.EqualFold(info.Type, "UNIQUE") {
			continue
		}
		names := held[semantics.QualifiedTableIdentityKey(info.TableName)]
		if !slices.ContainsFunc(names, func(name string) bool { return columnkey.Same(dialect, name, info.Name) }) {
			continue
		}
		identity := info.Identity
		if identity == (difftypes.ConstraintIdentity{}) {
			identity = constraintscope.Identity(semantics, info.TableName, info.Name)
		}
		if dropped == nil {
			dropped = make(map[difftypes.ConstraintIdentity]struct{})
		}
		dropped[identity] = struct{}{}
	}
	return dropped
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
